/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2018-2021 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.regionmerger.mode;

import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.common.math.BinMath;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.regionmerger.util.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class FindMissing implements Mode {
    protected static final Option<Integer> MIN_X = Option.integer("-minX", Integer.MIN_VALUE);
    protected static final Option<Integer> MIN_Z = Option.integer("-minZ", Integer.MIN_VALUE);
    protected static final Option<Integer> MAX_X = Option.integer("-maxX", Integer.MIN_VALUE);
    protected static final Option<Integer> MAX_Z = Option.integer("-maxZ", Integer.MIN_VALUE);
    protected static final Option<Boolean> REGION = Option.flag("r");
    protected static final Option<Boolean> OVERWRITE = Option.flag("o");
    protected static final Option<Format> FORMAT = Option.ofEnum("-format", Format.class, Format.MISSINGCHUNKS_JSON);
    protected static final Option<String> OUTPUT = Option.text("-output", "missingchunks.json");

    protected static final OpenOption[] REGION_OPEN_OPTIONS = { StandardOpenOption.READ };
    protected static final OpenOption[] MISSINGCHUNKS_JSON_OPEN_OPTIONS = { StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  findmissing:")
                .info("    Searches for missing chunks within a given area and saves them to a file.")
                .info("")
                .info("    Usage:")
                .info("      findmissing [options] <paths...>")
                .info("")
                .info("    Options:")
                .info("      --minX <minX>       Set the min X coord to check (in regions) (inclusive)")
                .info("      --minZ <minZ>       Set the min Z coord to check (in regions) (inclusive)")
                .info("      --maxX <minX>       Set the max X coord to check (in regions) (inclusive)")
                .info("      --maxZ <minZ>       Set the max Z coord to check (in regions) (inclusive)")
                .info("      --format <format>   Set the output format that will be used. Available options: missingchunks_json, geojson, geojson_negate_y")
                .info("                          Default: missingchunks_json")
                .info("      --output <file>     Sets the file that data will be written to. Default: missingchunks.json")
                .info("      -r                  Scans for missing regions rather than missing chunks.")
                .info("      -o                  Allows overwriting an existing output file.");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(false, true, MIN_X, MIN_Z, MAX_X, MAX_Z, OVERWRITE, FORMAT, REGION, OUTPUT);
    }

    @Override
    public String name() {
        return "findmissing";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final List<World> sources = args.getSources();

        final int minX = args.get(MIN_X);
        final int minZ = args.get(MIN_Z);
        final int maxX = args.get(MAX_X);
        final int maxZ = args.get(MAX_Z);
        if (min(minX, min(minZ, min(maxX, maxZ))) == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("minX, minZ, maxX and maxZ must all be set!");
        } else if (maxX < minX) {
            throw new IllegalArgumentException("maxX must be greater than or equal to minX!");
        } else if (maxZ < minZ) {
            throw new IllegalArgumentException("maxZ must be greater than or equal to minZ!");
        }

        Set<Vec2i> positions = new HashSet<>(sources.stream().map(World::regions).mapToInt(Collection::size).sum());
        sources.stream().map(World::regions).forEach(positions::addAll);

        logger.info("Loaded %d input worlds with a total of %d distinct regions.", sources.size(), positions.size());

        final int deltaX = (maxX + 1) - minX;
        final int deltaZ = (maxZ + 1) - minZ;

        final Format format = args.get(FORMAT);
        final boolean region = args.get(REGION);

        final File missingChunksJson = new File(args.get(OUTPUT));
        if (PFiles.checkFileExists(missingChunksJson) && !args.get(OVERWRITE)) {
            throw new IllegalStateException(missingChunksJson + " already exists (use -o to allow overwriting)");
        }

        try (FileChannel missingChunksJsonChannel = FileChannel.open(missingChunksJson.toPath(), MISSINGCHUNKS_JSON_OPEN_OPTIONS);
             FileLock lock = missingChunksJsonChannel.tryLock()) {
            checkState(lock != null, "Unable to obtain lock on missingchunks.json!");

            Stream<Vec2i> searchPositions = LongStream.rangeClosed(minX, maxX)
                    .flatMap(x -> IntStream.rangeClosed(minZ, maxZ).mapToLong(z -> BinMath.packXY((int) x, z)))
                    .mapToObj(pos -> new Vec2i(BinMath.unpackX(pos), BinMath.unpackY(pos)));

            Stream<Vec2i> missingPositionsStream;
            if (region) {
                missingPositionsStream = searchPositions.filter(pos -> !positions.contains(pos));
            } else {
                ThreadLocal<MappedByteBuffer[]> BUFFER_ARRAY_CACHE = ThreadLocal.withInitial(() -> new MappedByteBuffer[sources.size()]);
                ThreadLocal<Vec2i[]> VEC2I_ARRAY_CACHE = ThreadLocal.withInitial(() -> new Vec2i[32 * 32]);
                missingPositionsStream = searchPositions.parallel()
                        .flatMap((IOFunction<Vec2i, Stream<Vec2i>>) regionPos -> {
                            MappedByteBuffer[] buf = BUFFER_ARRAY_CACHE.get();
                            int bufCount = 0;

                            try {
                                for (World world : sources) {
                                    if (world.regions().contains(regionPos)) {
                                        try (FileChannel channel = FileChannel.open(world.getAsFile(regionPos).toPath(), REGION_OPEN_OPTIONS)) {
                                            if (channel.size() < SECTOR_BYTES) {
                                                continue;
                                            }
                                            buf[bufCount++] = channel.map(FileChannel.MapMode.READ_ONLY, 0L, 4096L);
                                        }
                                    }
                                }

                                if (bufCount > 0) {
                                    int missing = 0;
                                    Vec2i[] arr = VEC2I_ARRAY_CACHE.get();
                                    for (int x = 31; x >= 0; x--) {
                                        LOOP_Z:
                                        for (int z = 31; z >= 0; z--) {
                                            for (int i = bufCount - 1; i >= 0; i--) {
                                                if (buf[i].getInt(getOffsetIndex(x, z)) != 0) {
                                                    continue LOOP_Z;
                                                }
                                            }
                                            arr[missing++] = new Vec2i(regionPos.getX() * 32 + x, regionPos.getY() * 32 + z);
                                        }
                                    }
                                    return missing == 0 ? Stream.empty() : Arrays.stream(Arrays.copyOf(arr, missing));
                                } else {
                                    //region doesn't exist in any input, so all chunks are missing
                                    Vec2i[] result = new Vec2i[32 * 32];
                                    for (int x = 31; x >= 0; x--) {
                                        for (int z = 31; z >= 0; z--) {
                                            result[x * 32 + z] = new Vec2i(regionPos.getX() * 32 + x, regionPos.getY() * 32 + z);
                                        }
                                    }
                                    return Arrays.stream(result);
                                }
                            } finally {
                                while (bufCount-- != 0) {
                                    PUnsafe.pork_releaseBuffer(buf[bufCount]);
                                    buf[bufCount] = null;
                                }
                            }
                        });
            }

            byte[] json = missingPositionsStream.collect(format.collector()).getBytes(StandardCharsets.UTF_8);

            int written = missingChunksJsonChannel.write(ByteBuffer.wrap(json));
            if (written != json.length) {
                throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, json.length));
            }
        }
    }

    private enum Format {
        MISSINGCHUNKS_JSON {
            @Override
            Collector<Vec2i, ?, String> collector() {
                return Collectors.mapping(
                        pos -> PStrings.fastFormat("{\"x\":%d,\"z\":%d}", pos.getX(), pos.getY()),
                        Collectors.joining(",\n    ", "[\n    ", "\n]"));
            }
        },
        GEOJSON {
            @Override
            Collector<Vec2i, ?, String> collector() {
                return Collectors.mapping(
                        pos -> PStrings.fastFormat(
                                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[%1$d,%2$d]},\"properties\":{\"x\":%1$d,\"z\":%2$d}}",
                                pos.getX(), pos.getY()),
                        Collectors.joining(",", "{\"type\":\"FeatureCollection\",\"features\":[", "]}"));
            }
        },
        GEOJSON_NEGATE_Y {
            @Override
            Collector<Vec2i, ?, String> collector() {
                return Collectors.mapping(
                        pos -> PStrings.fastFormat(
                                "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[%1$d,%2$d]},\"properties\":{\"x\":%1$d,\"z\":%3$d}}",
                                pos.getX(), -pos.getY(), pos.getY()),
                        Collectors.joining(",", "{\"type\":\"FeatureCollection\",\"features\":[", "]}"));
            }
        };

        abstract Collector<Vec2i, ?, String> collector();
    }
}

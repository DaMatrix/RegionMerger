/*
 * Adapted from the Wizardry License
 *
 * Copyright (c) 2018-2019 DaPorkchop_ and contributors
 *
 * Permission is hereby granted to any persons and/or organizations using this software to copy, modify, merge, publish, and distribute it. Said persons and/or organizations are not allowed to use the software or any derivatives of the work for commercial use or any other means to generate income, nor are they allowed to claim this software as their own.
 *
 * The persons and/or organizations are also disallowed from sub-licensing and/or trademarking this software without explicit permission from DaPorkchop_.
 *
 * Any persons and/or organizations using this software must disclose their source code and have it publicly available, include this license, provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.regionmerger.mode.findmissing;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.OverclockedRegionFile;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.min;
import static net.daporkchop.regionmerger.anvil.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class FindMissing implements Mode {
    protected static final Option.Int  MIN_X     = Option.integer("-minX", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int  MIN_Z     = Option.integer("-minZ", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int  MAX_X     = Option.integer("-maxX", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int  MAX_Z     = Option.integer("-maxZ", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Flag OVERWRITE = Option.flag("o");

    protected static final OpenOption[] REGION_OPEN_OPTIONS = {StandardOpenOption.READ};
    protected static final OpenOption[] MISSINGCHUNKS_JSON_OPEN_OPTIONS = {StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("findmissing:")
                .info("  Searches for missing chunks within a given area and saves them to a file (missingchunks.json).")
                .info("")
                .info("  Usage: findmissing [options] <path>")
                .info("")
                .info("  Options:")
                .info("  --minX <minX>       Set the min X coord to check (in regions) (inclusive)")
                .info("  --minZ <minZ>       Set the min Z coord to check (in regions) (inclusive)")
                .info("  --maxX <minX>       Set the max X coord to check (in regions) (inclusive)")
                .info("  --maxZ <minZ>       Set the max Z coord to check (in regions) (inclusive)")
                .info("  -o                  Allows overwriting an existing missingchunks.json file.");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, false, MIN_X, MIN_Z, MAX_X, MAX_Z, OVERWRITE);
    }

    @Override
    public String name() {
        return "findmissing";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        if (false) {
            throw new UnsupportedOperationException("findmissing mode is currently unimplemented.");
        }

        final int minX = args.get(MIN_X);
        final int minZ = args.get(MIN_Z);
        final int maxX = args.get(MAX_X);
        final int maxZ = args.get(MAX_Z);
        final World world = args.getDestination();
        if (min(minX, min(minZ, min(maxX, maxZ))) == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("minX, minZ, maxX and maxZ must all be set!");
        } else if (maxX < minX) {
            throw new IllegalArgumentException("maxX must be greater than or equal to minX!");
        } else if (maxZ < minZ) {
            throw new IllegalArgumentException("maxZ must be greater than or equal to minZ!");
        }

        final int deltaX = (maxX + 1) - minX;
        final int deltaZ = (maxZ + 1) - minZ;

        final File missingChunksJson = new File("missingchunks.json");
        if (missingChunksJson.exists() && !args.get(OVERWRITE)) {
            throw new IllegalStateException("missingchunks.json already exists (use -o to allow overwriting)");
        }
        try (FileChannel missingChunksJsonChannel = FileChannel.open(missingChunksJson.toPath(), MISSINGCHUNKS_JSON_OPEN_OPTIONS)) {
            if (missingChunksJsonChannel.tryLock() == null) {
                throw new IllegalStateException("Unable to obtain lock on missingchunks.json!");
            }

            Vec2i[] searchPositions = new Vec2i[deltaX * deltaZ];
            for (int x = deltaX - 1; x >= 0; x--) {
                for (int z = deltaZ - 1; z >= 0; z--) {
                    searchPositions[x * deltaZ + z] = new Vec2i(minX + x, minZ + z);
                }
            }

            ThreadLocal<Vec2i[]> VEC2I_ARRAY_CACHE = ThreadLocal.withInitial(() -> new Vec2i[32 * 32]);
            byte[] json = Arrays.stream(searchPositions).parallel()
                    .flatMap((IOFunction<Vec2i, Stream<Vec2i>>) regionPos -> {
                        if (world.regions().contains(regionPos)) {
                            //region exists, scan the chunks
                            ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(SECTOR_BYTES);
                            try {
                                try (FileChannel channel = FileChannel.open(world.getAsFile(regionPos).toPath(), REGION_OPEN_OPTIONS)) {
                                    if (buf.writeBytes(channel, 0L, SECTOR_BYTES) != SECTOR_BYTES) {
                                        throw new IllegalStateException(String.format("Only read %d/%d bytes!", buf.writerIndex(), SECTOR_BYTES));
                                    }
                                }

                                int missing = 0;
                                Vec2i[] arr = VEC2I_ARRAY_CACHE.get();
                                for (int x = 31; x >= 0; x--) {
                                    for (int z = 31; z >= 0; z--) {
                                        if (buf.getInt((x << 2) | (z << 7)) == 0)   {
                                            arr[missing++] = new Vec2i(regionPos.getX() * 32 + x, regionPos.getY() * 32 + z);
                                        }
                                    }
                                }
                                return missing == 0 ? Stream.empty() : Arrays.stream(Arrays.copyOf(arr, missing));
                                /*return Arrays.stream(IntStream.range(0, 32 * 32)
                                        .filter(i -> buf.getInt(i << 2) == 0)
                                        .mapToObj(i -> new Vec2i(regionPos.getX() * 32 + (i & 0x1F), regionPos.getY() * 32 + ((i >> 5) & 0x1F)))
                                        .toArray(Vec2i[]::new));*/
                            } finally {
                                buf.release();
                            }
                        } else {
                            //region doesn't exist, so all chunks are missing
                            Vec2i[] result = new Vec2i[32 * 32];
                            for (int x = 31; x >= 0; x--) {
                                for (int z = 31; z >= 0; z--) {
                                    result[x * 32 + z] = new Vec2i(regionPos.getX() * 32 + x, regionPos.getY() * 32 + z);
                                }
                            }
                            return Arrays.stream(result);
                        }
                    })
                    .map(v -> String.format("{\"x\":%d,\"z\":%d}", v.getX(), v.getY()))
                    .collect(() -> new StringJoiner(",\n    ", "[\n    ", "\n]"), StringJoiner::add, StringJoiner::merge)
                    .toString().getBytes(StandardCharsets.UTF_8);

            int written = missingChunksJsonChannel.write(ByteBuffer.wrap(json));
            if (written != json.length) {
                throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, json.length));
            }
        }
    }
}

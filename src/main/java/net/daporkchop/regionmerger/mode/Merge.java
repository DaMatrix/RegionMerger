/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2018-2020 DaPorkchop_
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
 */

package net.daporkchop.regionmerger.mode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.daporkchop.lib.minecraft.world.format.anvil.region.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class Merge implements Mode {
    protected static final Option.Int PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

    protected static final OpenOption[] READ_OPEN_OPTIONS  = {StandardOpenOption.READ};
    protected static final OpenOption[] WRITE_OPEN_OPTIONS = {StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW};

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  merge:")
                .info("    Combines multiple source worlds into a single, new destination world.")
                .info("    Note that all paths are to the region directory, not the world directory.")
                .info("")
                .info("    Usage:")
                .info("      merge [options] <destination> <source> [source]...")
                .info("")
                .info("    Options:")
                .info("      -p <time>   Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, true, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "merge";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final World dst = args.getDestination();
        final List<World> sources = args.getSources();

        if (!dst.regions().isEmpty()) {
            logger.alert("Destination (\"%s\") has existing regions!\nConsider using add mode to add chunks onto the existing world.");
            System.exit(1);
        }

        Collection<Vec2i> regionPositions = sources.stream()
                .map(World::regions)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toSet());

        logger.info("Loaded %d input worlds with a total of %d distinct regions.", sources.size(), regionPositions.size());

        long time = System.currentTimeMillis();
        AtomicLong remainingRegions = new AtomicLong(regionPositions.size());
        AtomicLong totalChunks = new AtomicLong(0L);

        {
            final int delay = args.get(PROGRESS_UPDATE_DELAY);
            if (delay > 0) {
                Thread t = new Thread((ERunnable) () -> {
                    Logger channel = logger.channel("Progress");
                    int total = regionPositions.size();
                    while (true) {
                        Thread.sleep(delay);
                        long remaining = remainingRegions.get();
                        channel.info("Copied %d/%d regions (%.3f%%, %d chunks)", total - remaining, total, (float) (total - remaining) / (float) total * 100.0f, totalChunks.get());
                        if (remaining == 0) {
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }
        }

        ThreadLocal<ByteBuf[]> REGIONS_CACHE = ThreadLocal.withInitial(() -> new ByteBuf[sources.size()]);
        ThreadLocal<List<String>> FILENAMES_CACHE = ThreadLocal.withInitial(ArrayList::new);
        regionPositions.parallelStream().forEach((IOConsumer<Vec2i>) pos -> {
            /*List<OverclockedRegionFile> regions = new LinkedList<>();
            for (int i = 0; i < sources.size(); i++) {
                World world = sources.get(i);
                if (world.regions().contains(pos)) {
                    regions.add(new OverclockedRegionFile(world.getAsFile(pos), true, false));
                }
            }*/
            ByteBuf[] regions = REGIONS_CACHE.get();
            List<String> filenames = FILENAMES_CACHE.get();
            filenames.clear();
            int regionsCount = 0;
            for (int i = 0; i < regions.length; i++) {
                World world = sources.get(i);
                if (world.regions().contains(pos)) {
                    ByteBuf buf = null;
                    File f = world.getAsFile(pos);
                    try (FileChannel channel = FileChannel.open(f.toPath(), READ_OPEN_OPTIONS)) {
                        long size = channel.size();
                        if (size > Integer.MAX_VALUE) {
                            throw new IllegalStateException(String.format("Region too big: %s (%d bytes)", f.getAbsolutePath(), size));
                        } else if (size < HEADER_BYTES) {
                            throw new IllegalStateException(String.format("Region too small: %s (%d bytes)", f.getAbsolutePath(), size));
                        }
                        buf = PooledByteBufAllocator.DEFAULT.ioBuffer((int) size);
                        int cnt = buf.writeBytes(channel, (int) size);
                        if (cnt != size || buf.writerIndex() != size) {
                            throw new IllegalStateException(String.format("Only read %d (%d)/%d bytes!", cnt, buf.writerIndex(), size));
                        }
                    } catch (Exception e) {
                        logger.warn(e);
                        if (buf != null) {
                            buf.release();
                        }
                        if (e instanceof RuntimeException) {
                            throw e;
                        } else {
                            continue;
                        }
                    }
                    filenames.add(f.getAbsolutePath());
                    regions[regionsCount++] = buf;
                }
            }

            logger.debug("Copying region (%d,%d) using input regions: %s", pos.getX(), pos.getY(), IntStream.range(0, regionsCount).collect(
                    () -> new StringJoiner(", ", "[", "]"),
                    (joiner, i) -> joiner.add(String.format("\"%s\" (%d bytes)", filenames.get(i), regions[i].writerIndex())),
                    StringJoiner::merge
            ).toString());

            ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(SECTOR_BYTES * (2 + 32 * 32)).writeBytes(EMPTY_SECTOR).writeBytes(EMPTY_SECTOR);
            try {
                int sector = 2;
                int chunks = 0;
                for (int x = 31; x >= 0; x--) {
                    for (int z = 31; z >= 0; z--) {
                        final int offset = getOffsetIndex(x, z);
                        for (int i = 0; i < regionsCount; i++) {
                            try {
                                ByteBuf region = regions[i];
                                final int chunkOffset = region.getInt(offset);
                                if (chunkOffset != 0) {
                                    final int chunkPos = (chunkOffset >>> 8) * SECTOR_BYTES;
                                    final int sizeBytes = region.getInt(chunkPos);

                                    buf.setInt(offset + SECTOR_BYTES, region.getInt(offset + SECTOR_BYTES)); //copy timestamp

                                    buf.writeBytes(region, chunkPos, sizeBytes + LENGTH_HEADER_SIZE); //copy chunk data
                                    buf.writeBytes(EMPTY_SECTOR, 0, ((buf.writerIndex() - 1 >> 12) + 1 << 12) - buf.writerIndex()); //pad to next sector

                                    final int chunkSectors = (buf.writerIndex() - 1 >> 12) + 1; //compute next chunk sector
                                    buf.setInt(offset, (chunkSectors - sector) | (sector << 8)); //set offset value in region header
                                    sector = chunkSectors;
                                    chunks++;
                                    break;
                                }
                            } catch (IndexOutOfBoundsException e)   {
                                StringJoiner joiner = new StringJoiner("\n");
                                Logger.getStackTrace(e, joiner::add);
                                //i belive this is caused by corruption
                                logger.alert("%s\n\nThis is most likely caused by corruption!\n\nCaused at (%d,%d) in (%d,%d): \"%s\"", joiner, x, z, pos.getX(), pos.getY(), filenames.get(i));
                            }
                        }
                    }
                }
                if (chunks > 0) {
                    try (FileChannel channel = FileChannel.open(dst.getAsFile(pos).toPath(), WRITE_OPEN_OPTIONS)) {
                        int writeable = buf.readableBytes();
                        int written = buf.readBytes(channel, writeable);
                        if (writeable != written) {
                            throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, writeable));
                        }
                    }
                    totalChunks.getAndAdd(chunks);
                } else {
                    logger.warn("Found no input chunks for region (%d,%d)", pos.getX(), pos.getY());
                }
                remainingRegions.getAndDecrement();
            } catch (Exception e)   {
                logger.alert("%s\n\nWhile processing region (%d,%d)", e, pos.getX(), pos.getY());
                PUnsafe.throwException(e);
            } finally {
                buf.release();
                while (--regionsCount >= 0) {
                    regions[regionsCount].release();
                    regions[regionsCount] = null;
                }
            }
        });

        time = System.currentTimeMillis() - time;
        logger.success(
                "Copied %d chunks (%.2f MB) in %02dh %02dm %02ds %03dms",
                totalChunks.get(),
                regionPositions.stream().map(dst::getAsFile).mapToLong(File::length).sum() / (1024.0d * 1024.0d),
                time / (60L * 60L * 1000L),
                (time / (60L * 1000L)) % 60L,
                (time / 1000L) % 60L,
                time % 1000L
        );
    }
}

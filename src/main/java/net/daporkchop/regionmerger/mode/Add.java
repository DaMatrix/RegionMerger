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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.util.Sort;
import net.daporkchop.regionmerger.util.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;
import static net.daporkchop.regionmerger.Main.*;

/**
 * @author DaPorkchop_
 */
public class Add implements Mode {
    protected static final Option<Integer> PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);
    protected static final Option<Sort> SORT = Option.ofEnum("-sort", Sort.class, Sort.YOUNGEST);

    protected static final OpenOption[] READ_OPEN_OPTIONS = { StandardOpenOption.READ };
    protected static final OpenOption[] WRITE_OPEN_OPTIONS = { StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  add:")
                .info("    Copies all distinct chunks from multiple source worlds into a single destination world.")
                .info("")
                .info("    Usage:")
                .info("      add [options] <destination> <source> [source]...")
                .info("")
                .info("    Options:")
                .info("      --sort <sort>  Sets the algorithm used to sort individual chunks when multiple candidates exist.")
                .info("                     Options: youngest, oldest, input_order. Default: youngest")
                .info("      -p <time>      Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, true, SORT, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "add";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final World dst = args.getDestination();
        final List<World> sources = args.getSources();

        final Sort sort = args.get(SORT);

        Collection<Vec2i> regionPositions = sources.stream()
                .map(World::regions)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        logger.info("Loaded output world with %d existing regions.", dst.regions().size());
        logger.info("Loaded %d input worlds with a total of %d distinct regions.", sources.size(), regionPositions.size());
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
                        channel.info("Processed %d/%d regions (%.3f%%, %d chunks)", total - remaining, total, (float) (total - remaining) / (float) total * 100.0f, totalChunks.get());
                        if (remaining == 0) {
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }
        }

        ThreadLocal<ByteBuf[]> REGIONS_CACHE = ThreadLocal.withInitial(() -> new ByteBuf[sources.size() + 1]);
        regionPositions.parallelStream()
                .forEach((IOConsumer<Vec2i>) pos -> {
                    File dstFile = dst.getAsFile(pos);
                    ByteBuf[] regions = REGIONS_CACHE.get();
                    int regionsCount = 0;
                    BASE:
                    if (dst.regions().contains(pos)) {
                        ByteBuf buf = null;
                        try (FileChannel channel = FileChannel.open(dstFile.toPath(), READ_OPEN_OPTIONS)) {
                            long size = channel.size();
                            if (size > Integer.MAX_VALUE) {
                                throw new IllegalStateException(String.format("Region too big: %s (%d bytes)", dst.getAsFile(pos).getAbsolutePath(), size));
                            } else if (size < HEADER_BYTES) {
                                throw new IllegalStateException(String.format("Region too small: %s (%d bytes)", dst.getAsFile(pos).getAbsolutePath(), size));
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
                                break BASE;
                            }
                        }
                        regions[regionsCount++] = buf;
                    }
                    for (int i = 0; i < regions.length - 1; i++) {
                        World world = sources.get(i);
                        if (world.regions().contains(pos)) {
                            ByteBuf buf = null;
                            try (FileChannel channel = FileChannel.open(world.getAsFile(pos).toPath(), READ_OPEN_OPTIONS)) {
                                long size = channel.size();
                                if (size > Integer.MAX_VALUE) {
                                    throw new IllegalStateException(String.format("Region too big: %s (%d bytes)", world.getAsFile(pos).getAbsolutePath(), size));
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
                            regions[regionsCount++] = buf;
                        }
                    }

                    int chunks = 0;
                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(SECTOR_BYTES * (2 + 32 * 32)).writeBytes(EMPTY_HEADERS);
                    try {
                        try {
                            int sector = 2;
                            for (int x = 0; x < 32; x++) {
                                for (int z = 0; z < 32; z++) {
                                    ByteBuf region = sort.select(regions, regionsCount, x, z);

                                    if (region != null) {
                                        final int offsetIndex = getOffsetIndex(x, z);
                                        final int timestampIndex = getTimestampIndex(x, z);

                                        int chunkOffset = region.getInt(offsetIndex);
                                        final int chunkPos = (chunkOffset >>> 8) * SECTOR_BYTES;
                                        final int sizeBytes = region.getInt(chunkPos);

                                        buf.setInt(timestampIndex, region.getInt(timestampIndex)); //copy timestamp

                                        buf.writeBytes(region, chunkPos, sizeBytes + 4);
                                        buf.writeBytes(EMPTY_SECTOR, 0, ((buf.writerIndex() - 1 >> 12) + 1 << 12) - buf.writerIndex()); //pad to next sector

                                        final int chunkSectors = (buf.writerIndex() - 1 >> 12) + 1; //compute next chunk sector
                                        buf.setInt(offsetIndex, (chunkSectors - sector) | (sector << 8)); //set offset value in region header
                                        sector = chunkSectors;
                                        chunks++;
                                    }
                                }
                            }
                        } finally {
                            while (--regionsCount >= 0) {
                                regions[regionsCount].release();
                                regions[regionsCount] = null;
                            }
                        }
                        if (chunks > 0) {
                            try (FileChannel channel = FileChannel.open(dstFile.toPath(), WRITE_OPEN_OPTIONS)) {
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
                    } finally {
                        buf.release();
                    }
                    remainingRegions.getAndDecrement();
                });

        logger.success(
                "Copied %d chunks (%.2f MB)",
                totalChunks.get(),
                regionPositions.stream().map(dst::getAsFile).mapToLong(File::length).sum() / (1024.0d * 1024.0d));
    }
}

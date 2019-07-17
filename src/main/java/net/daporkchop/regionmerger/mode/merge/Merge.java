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

package net.daporkchop.regionmerger.mode.merge;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.OverclockedRegionFile;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
public class Merge implements Mode {
    protected static final Option.Int PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("merge:")
                .info("  Copies all distinct chunks from multiple source worlds into a single destination world.")
                .info("  Note that all paths are to the region directory, not the world directory.")
                .info("")
                .info("  Usage: merge <destination> <source> <source>...")
                .info("")
                .info("  Options:")
                .info("  -p <time>   Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
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

        if (!dst.regions().isEmpty())    {
            throw new IllegalStateException("Destination has existing regions!");
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
            if (delay > 0)  {
                Thread t = new Thread((ERunnable) () -> {
                    Logger channel = logger.channel("Progress");
                    int total = regionPositions.size();
                    while (true)   {
                        Thread.sleep(delay);
                        long remaining = remainingRegions.get();
                        channel.info("Copied %d/%d regions (%.3f%%, %d chunks)", total - remaining, total, (float) (total - remaining) / (float) total, totalChunks.get());
                        if (remaining == 0) {
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }
        }

        regionPositions.parallelStream().forEach((IOConsumer<Vec2i>) pos -> {
            List<OverclockedRegionFile> regions = new LinkedList<>();
            for (int i = 0; i < sources.size(); i++) {
                World world = sources.get(i);
                if (world.regions().contains(pos)) {
                    regions.add(new OverclockedRegionFile(world.getAsFile(pos), true, false));
                }
            }

            ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(OverclockedRegionFile.SECTOR_BYTES * 3).writerIndex(OverclockedRegionFile.SECTOR_BYTES * 2);
            try {
                int sector = 2;
                int chunks = 0;
                for (int x = 31; x >= 0; x--) {
                    for (int z = 31; z >= 0; z--) {
                        FIND_REGION_WITH_CHUNK:
                        for (int i = 0; i < regions.size(); i++)    {
                            OverclockedRegionFile region = regions.get(i);
                            if (region.hasChunk(x, z))  {
                                ByteBuf chunk = region.readDirect(x, z);
                                try {
                                    int cnt = chunk.readableBytes();
                                    buf.writeBytes(chunk);
                                    if (chunk.isReadable()) {
                                        throw new IllegalStateException("Couldn't copy entire chunk into output buffer!");
                                    }
                                    cnt = cnt / OverclockedRegionFile.SECTOR_BYTES + 1;
                                    buf.setInt((x << 2) | (z << 7), cnt | (sector << 8));
                                    sector += cnt;
                                    buf.writerIndex(OverclockedRegionFile.SECTOR_BYTES * sector);
                                    chunks++;
                                    break FIND_REGION_WITH_CHUNK;
                                } finally {
                                    chunk.release();
                                }
                            }
                        }
                    }
                }
                if (chunks > 0) {
                    try (FileChannel channel = FileChannel.open(dst.getAsFile(pos).toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
                        int writeable = buf.readableBytes();
                        int written = buf.readBytes(channel, writeable);
                        if (writeable != written) {
                            throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, writeable));
                        }
                    }
                    remainingRegions.getAndDecrement();
                    totalChunks.getAndAdd(chunks);
                }
            } finally {
                buf.release();
                regions.forEach((IOConsumer<OverclockedRegionFile>) OverclockedRegionFile::close);
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

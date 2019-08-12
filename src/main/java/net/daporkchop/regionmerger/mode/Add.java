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

package net.daporkchop.regionmerger.mode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.OverclockedRegionFile;
import net.daporkchop.regionmerger.anvil.ex.CannotOpenRegionException;
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

import static net.daporkchop.regionmerger.anvil.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class Add implements Mode {
    protected static final Option.Flag KEEP_EXISTING         = Option.flag("k");
    protected static final Option.Flag TURBO                 = Option.flag("t");
    protected static final Option.Int  PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

    protected static final OpenOption[] WRITE_OPEN_OPTIONS = {StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  add:")
                .info("    Copies all distinct chunks from multiple source worlds into a single destination world. Similar to merge, except that it")
                .info("    can add chunks to an existing world and is a fair bit slower.")
                .info("    Note that all paths are to the region directory, not the world directory.")
                .info("")
                .info("    Usage:")
                .info("      add [options] <destination> <source> [source]...")
                .info("")
                .info("    Options:")
                .info("      -k          Prevents overwriting existing chunks in the destination world.")
                .info("      -t          Uses an alternative merging algorithm can be significantly faster, but may cause issues with corruption. Make")
                .info("                  sure to take backups when using this!")
                .info("      -p <time>   Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, true, KEEP_EXISTING, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "add";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final World dst = args.getDestination();
        final List<World> sources = args.getSources();

        final boolean keepExisting = args.get(KEEP_EXISTING);
        final boolean turbo = args.get(TURBO);

        if (turbo && keepExisting)  {
            logger.error("-t and -k are not compatible!");
            System.exit(1);
        }

        Collection<Vec2i> regionPositions = sources.stream()
                .map(World::regions)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toSet());

        logger.info("Loaded output world with %d existing regions.", dst.regions().size());
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

        if (args.get(TURBO))    {
            ThreadLocal<OverclockedRegionFile[]> REGIONS_CACHE = ThreadLocal.withInitial(() -> new OverclockedRegionFile[sources.size() + 1]);
            regionPositions.parallelStream()
                    .forEach((IOConsumer<Vec2i>) pos -> {
                        File dstFile = dst.getAsFile(pos);
                        OverclockedRegionFile[] regions = REGIONS_CACHE.get();
                        int regionsCount;
                        if (dst.regions().contains(pos))    {
                            regionsCount = 1;
                            regions[0] = new OverclockedRegionFile(dstFile, true, false);
                        } else {
                            regionsCount = 0;
                        }
                        for (int i = 0; i < regions.length; i++) {
                            World world = sources.get(i);
                            if (world.regions().contains(pos)) {
                                OverclockedRegionFile region;
                                try {
                                    region = new OverclockedRegionFile(world.getAsFile(pos), true, false);
                                } catch (CannotOpenRegionException e) {
                                    logger.warn(e);
                                    continue;
                                }
                                regions[regionsCount++] = region;
                            }
                        }

                        int chunks = 0;
                        ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(
                                SECTOR_BYTES * (2 + 32 * 32),
                                SECTOR_BYTES * ((32 * 32) * 256 + 2)
                        ).writeBytes(EMPTY_SECTOR).writeBytes(EMPTY_SECTOR);
                        try {
                            int sector = 2;
                            for (int x = 31; x >= 0; x--) {
                                for (int z = 31; z >= 0; z--) {
                                    for (int i = 0; i < regionsCount; i++) {
                                        OverclockedRegionFile region = regions[i];
                                        if (region.hasChunk(x, z)) {
                                            buf.writeBytes(EMPTY_SECTOR, 0, (~buf.capacity() & 0xFFF) + 1); //pad to next sector
                                            //we do this before appending the chunk so that the final chunk in the region doesn't get padded, this does nothing the first time

                                            ByteBuf chunk = region.readDirect(x, z);
                                            try {
                                                int cnt = chunk.readableBytes();
                                                buf.writeBytes(chunk);
                                                if (chunk.isReadable()) {
                                                    throw new IllegalStateException("Couldn't copy entire chunk into output buffer!");
                                                }
                                                cnt = cnt / SECTOR_BYTES + 1;
                                                buf.setInt(getOffsetIndex(x, z), cnt | (sector << 8));
                                                buf.setInt(getTimestampIndex(x, z), region.getTimestamp(x, z));
                                                sector += cnt;
                                                chunks++;
                                                break;
                                            } finally {
                                                chunk.release();
                                            }
                                        }
                                    }
                                }
                            }
                        } finally {
                            buf.release();
                            while (--regionsCount >= 0) {
                                regions[regionsCount].close();
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
                        remainingRegions.getAndDecrement();
                    });
        } else {
            ThreadLocal<OverclockedRegionFile[]> REGIONS_CACHE = ThreadLocal.withInitial(() -> new OverclockedRegionFile[sources.size()]);
            regionPositions.parallelStream()
                    .forEach((IOConsumer<Vec2i>) pos -> {
                        OverclockedRegionFile[] regions = REGIONS_CACHE.get();
                        int regionsCount = 0;
                        for (int i = 0; i < regions.length; i++) {
                            World world = sources.get(i);
                            if (world.regions().contains(pos)) {
                                OverclockedRegionFile region;
                                try {
                                    region = new OverclockedRegionFile(world.getAsFile(pos), true, false);
                                } catch (CannotOpenRegionException e) {
                                    logger.warn(e);
                                    continue;
                                }
                                regions[regionsCount++] = region;
                            }
                        }

                        try (OverclockedRegionFile dstRegion = new OverclockedRegionFile(dst.getAsFile(pos), false, true)) {
                            int chunks = 0;
                            if (keepExisting) {
                                for (int x = 31; x >= 0; x--) {
                                    for (int z = 31; z >= 0; z--) {
                                        if (!dstRegion.hasChunk(x, z)) { //only copy if destination region doesn't have the chunk
                                            for (int i = 0; i < regionsCount; i++) { //search for input region containing the chunk
                                                if (regions[i].hasChunk(x, z)) {
                                                    dstRegion.writeDirect(x, z, regions[i].readDirect(x, z));
                                                    dstRegion.setTimestamp(x, z, regions[i].getTimestamp(x, z));
                                                    chunks++;
                                                    break; //chunk has been found, continue to next one
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                for (int x = 31; x >= 0; x--) {
                                    for (int z = 31; z >= 0; z--) {
                                        for (int i = 0; i < regionsCount; i++) { //search for input region containing the chunk
                                            if (regions[i].hasChunk(x, z)) {
                                                dstRegion.writeDirect(x, z, regions[i].readDirect(x, z));
                                                dstRegion.setTimestamp(x, z, regions[i].getTimestamp(x, z));
                                                chunks++;
                                                break; //chunk has been found, continue to next one
                                            }
                                        }
                                    }
                                }
                            }
                            totalChunks.getAndAdd(chunks);
                            remainingRegions.getAndDecrement();
                        } finally {
                            while (--regionsCount >= 0) {
                                regions[regionsCount].close();
                                regions[regionsCount] = null;
                            }
                        }
                    });
        }

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

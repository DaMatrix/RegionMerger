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
import net.daporkchop.lib.compression.context.PDeflater;
import net.daporkchop.lib.compression.context.PInflater;
import net.daporkchop.lib.compression.zlib.Zlib;
import net.daporkchop.lib.compression.zlib.options.ZlibDeflaterOptions;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.mcworldlib.format.anvil.region.RawChunk;
import net.daporkchop.mcworldlib.format.anvil.region.RegionFile;
import net.daporkchop.mcworldlib.format.anvil.region.impl.MemoryMappedRegionFile;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;
import net.daporkchop.regionmerger.util.Utils;
import net.daporkchop.regionmerger.util.World;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;
import static net.daporkchop.regionmerger.Main.*;

/**
 * @author DaPorkchop_
 */
public class Optimize implements Mode {
    protected static final Option<Boolean> RECOMPRESS = Option.flag("c");
    protected static final Option<Integer> LEVEL = Option.integer("l", Zlib.LEVEL_DEFAULT);
    protected static final Option<Integer> PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

    protected static final OpenOption[] INPUT_OPEN_OPTIONS = { StandardOpenOption.READ };
    protected static final OpenOption[] OUTPUT_OPEN_OPTIONS = { StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING };

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  optimize:")
                .info("    Optimizes the size of a world by defragmenting and optionally re-compressing the regions.")
                .info("    This is only useful for worlds that will later be served read-only, as allowing the Minecraft client/server write access to an")
                .info("    optimized world will cause size to increase again.")
                .info("")
                .info("    Usage:")
                .info("      optimize [options] <path>")
                .info("")
                .info("    Options:")
                .info("      -c          Enables re-compression of chunks. This will significantly increase the runtime (and CPU usage), but can help decrease")
                .info("                  output size further.")
                .info("      -l <level>  Sets the level (intensity) of the compression, from 0-9. 1 is the worst, 9 is the best. 0 does not compression and-1 is automatic. Only effective")
                .info("                  -1 is automatic. Only effective with -c. Default: -1")
                .info("      -p <time>   Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, false, RECOMPRESS, LEVEL, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "optimize";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final boolean recompress = args.get(RECOMPRESS);
        final int level = args.get(LEVEL);
        final World world = args.getDestination();

        Collection<File> regionsAsFiles = world.regions().stream().map(world::getAsFile).collect(Collectors.toList());

        long initialSize = regionsAsFiles.parallelStream().mapToLong(File::length).sum();
        logger.info("Initial size: %.2f MB", initialSize / (1024.0d * 1024.0d));

        ChunkRecoder recoder;
        if (recompress) {
            ThreadLocal<PInflater> inflaterCache = ThreadLocal.withInitial(Zlib.PROVIDER::inflater);
            ZlibDeflaterOptions deflaterOptions = Zlib.PROVIDER.deflateOptions().withLevel(level);
            ThreadLocal<PDeflater> deflaterCache = ThreadLocal.withInitial(() -> Zlib.PROVIDER.deflater(deflaterOptions));
            recoder = (src, dst) -> {
                byte mode = src.readByte();
                if (mode != ID_ZLIB && mode != ID_GZIP) {
                    throw new IllegalArgumentException(String.format("Invalid chunk version: %d", mode & 0xFF));
                }

                int oldIndex = dst.writerIndex();
                dst.writeInt(-1).writeByte(ID_ZLIB);

                ByteBuf tmp = PooledByteBufAllocator.DEFAULT.ioBuffer(2097152);
                try {
                    inflaterCache.get().decompressGrowing(src, tmp);
                    deflaterCache.get().compressGrowing(tmp, dst);
                    dst.setInt(oldIndex, dst.writerIndex() - oldIndex - 4);
                } finally {
                    tmp.release();
                }
            };
            logger.info("Reordering and recompressing %d regions at DEFLATE level %d...", regionsAsFiles.size(), level);
        } else {
            //simply copy without anything else
            recoder = (src, dst) -> {
                dst.writeInt(src.readableBytes()).writeBytes(src);
                if (src.isReadable()) {
                    throw new IllegalStateException("Couldn't copy entire chunk into output buffer!");
                }
            };
            logger.info("Reordering %d regions...", regionsAsFiles.size());
        }

        AtomicLong remainingRegions = new AtomicLong(regionsAsFiles.size());
        AtomicLong totalChunks = new AtomicLong(0L);

        {
            final int delay = args.get(PROGRESS_UPDATE_DELAY);
            if (delay > 0) {
                Thread t = new Thread((ERunnable) () -> {
                    Logger channel = logger.channel("Progress");
                    int total = regionsAsFiles.size();
                    while (true) {
                        Thread.sleep(delay);
                        long remaining = remainingRegions.get();
                        channel.info("Optimized %d/%d regions (%.3f%%, %d chunks)", total - remaining, total, (float) (total - remaining) / (float) total * 100.0f, totalChunks.get());
                        if (remaining == 0) {
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }
        }

        regionsAsFiles.parallelStream().forEach((IOConsumer<File>) file -> {
            ByteBuf dst = PooledByteBufAllocator.DEFAULT.ioBuffer(SECTOR_BYTES * (2 + 32 * 32)).writeBytes(EMPTY_SECTOR).writeBytes(EMPTY_SECTOR);
            try {
                int sector = 2;
                int chunks = 0;

                try (RegionFile region = new MemoryMappedRegionFile(file, true)) {
                    for (int x = 0; x < 32; x++) {
                        for (int z = 0; z < 32; z++) {
                            try (RawChunk chunk = region.read(x, z)) {
                                if (chunk != null) { //chunk exists
                                    dst.setInt(getTimestampIndex(x, z), (int) (chunk.timestamp() / 1000L)); //copy timestamp

                                    recoder.recode(chunk.data(), dst);
                                    dst.writeBytes(EMPTY_SECTOR, 0, ((dst.writerIndex() - 1 >> 12) + 1 << 12) - dst.writerIndex()); //pad to next sector

                                    final int chunkSectors = (dst.writerIndex() - 1 >> 12) + 1; //compute next chunk sector
                                    dst.setInt(getOffsetIndex(x, z), (chunkSectors - sector) | (sector << 8)); //set offset value in region header
                                    sector = chunkSectors;
                                    chunks++;
                                }
                            }
                        }
                    }
                }

                if (chunks == 0 && !file.delete()) {
                    throw new IllegalStateException(String.format("Couldn't delete file \"%s\"!", file.getAbsolutePath()));
                } else {
                    Utils.writeAndReplace(file.toPath(), dst);
                }
                remainingRegions.getAndDecrement();
                totalChunks.getAndAdd(chunks);
            } finally {
                dst.release();
            }
        });

        int oldCount = regionsAsFiles.size();
        regionsAsFiles.removeIf(f -> !f.exists());
        long finalSize = regionsAsFiles.parallelStream().mapToLong(File::length).sum();
        logger.success("Processed %d regions (deleting %d empty regions)", oldCount, oldCount - regionsAsFiles.size());
        logger.success("Shrunk by %.2f MB (%.3f%%)", (initialSize - finalSize) / (1024.0d * 1024.0d), (1.0d - (double) finalSize / (double) initialSize) * 100.0d);
    }

    @FunctionalInterface
    private interface ChunkRecoder {
        void recode(@NonNull ByteBuf src, @NonNull ByteBuf dst) throws IOException;
    }
}

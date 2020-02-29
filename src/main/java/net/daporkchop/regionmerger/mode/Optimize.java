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
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.minecraft.world.format.anvil.region.RegionFile;
import net.daporkchop.lib.minecraft.world.format.anvil.region.RegionOpenOptions;
import net.daporkchop.lib.natives.PNatives;
import net.daporkchop.lib.natives.zlib.PDeflater;
import net.daporkchop.lib.natives.zlib.PInflater;
import net.daporkchop.lib.natives.zlib.Zlib;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import static net.daporkchop.lib.minecraft.world.format.anvil.region.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class Optimize implements Mode {
    protected static final Option.Flag RECOMPRESS            = Option.flag("c");
    protected static final Option.Int  LEVEL                 = Option.integer("l", 7, 0, 8);
    protected static final Option.Int  PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

    protected static final RegionOpenOptions REGION_OPEN_OPTIONS = new RegionOpenOptions()
            .mode(RegionFile.Mode.MMAP_FULL)
            .access(RegionFile.Access.READ_ONLY);

    protected static final OpenOption[] INPUT_OPEN_OPTIONS  = {StandardOpenOption.READ};
    protected static final OpenOption[] OUTPUT_OPEN_OPTIONS = {StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};

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
                .info("      -l <level>  Sets the level (intensity) of the compression, from 0-8. 0 is the worst, 8 is the best. Only effective with -c. Default: 7")
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
            ThreadLocal<PInflater> inflaterCache = ThreadLocal.withInitial(() -> PNatives.ZLIB.get().inflater(Zlib.ZLIB_MODE_AUTO));
            ThreadLocal<PDeflater> deflaterCache = ThreadLocal.withInitial(() -> PNatives.ZLIB.get().deflater(level));
            recoder = (src, dst) -> {
                PDeflater deflater = deflaterCache.get();
                PInflater inflater = inflaterCache.get();

                byte mode = src.readByte();
                if (mode != ID_ZLIB && mode != ID_GZIP) {
                    throw new IllegalArgumentException(String.format("Invalid chunk version: %d", mode & 0xFF));
                }

                int oldIndex = dst.writerIndex();
                dst.writeInt(-1).writeByte(ID_ZLIB);

                ByteBuf tmp = PooledByteBufAllocator.DEFAULT.ioBuffer(2097152);
                try {
                    inflater.inflate(src, tmp);
                    deflater.deflate(tmp, dst);
                    dst.setInt(oldIndex, dst.writerIndex() - oldIndex - 4);
                } finally {
                    tmp.release();
                    inflater.reset();
                    deflater.reset();
                }
            };
            logger.info("Reordering and recompressing %d regions at DEFLATE level %d...", regionsAsFiles.size(), level);
        } else {
            //simply copy without anything else
            recoder = (src, dst) -> {
                dst.writeBytes(src);
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

                try (RegionFile region = RegionFile.open(file, REGION_OPEN_OPTIONS)) {
                    for (int x = 31; x >= 0; x--) {
                        for (int z = 31; z >= 0; z--) {
                            if (!region.hasChunk(x, z)) {
                                continue;
                            }

                            ByteBuf chunk = region.readDirect(x, z);
                            try {
                                dst.setInt(getTimestampIndex(x, z), region.getTimestamp(x, z)); //copy timestamp

                                recoder.recode(chunk, dst);
                                dst.writeBytes(EMPTY_SECTOR, 0, ((dst.writerIndex() - 1 >> 12) + 1 << 12) - dst.writerIndex()); //pad to next sector

                                final int chunkSectors = (dst.writerIndex() - 1 >> 12) + 1; //compute next chunk sector
                                dst.setInt(getOffsetIndex(x, z), (chunkSectors - sector) | (sector << 8)); //set offset value in region header
                                sector = chunkSectors;
                                chunks++;
                            } finally {
                                chunk.release();
                            }
                        }
                    }
                }

                if (chunks == 0 && !file.delete()) {
                    throw new IllegalStateException(String.format("Couldn't delete file \"%s\"!", file.getAbsolutePath()));
                } else {
                    try (FileChannel channel = FileChannel.open(file.toPath(), OUTPUT_OPEN_OPTIONS)) {
                        int writeable = dst.readableBytes();
                        int written = dst.readBytes(channel, writeable);
                        if (writeable != written) {
                            throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, writeable));
                        }
                    }
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

    @UtilityClass
    private static class FastStreams {
        private final long FOS_OUT_OFFSET = PUnsafe.pork_getOffset(FilterOutputStream.class, "out");
        private final long IOS_INF_OFFSET = PUnsafe.pork_getOffset(InflaterOutputStream.class, "inf");
        private final long IOS_BUF_OFFSET = PUnsafe.pork_getOffset(InflaterOutputStream.class, "buf");
        private final long DOS_DEF_OFFSET = PUnsafe.pork_getOffset(DeflaterOutputStream.class, "def");
        private final long DOS_BUF_OFFSET = PUnsafe.pork_getOffset(DeflaterOutputStream.class, "buf");

        private final ThreadLocal<byte[][]> BUFFERS = ThreadLocal.withInitial(() -> new byte[2][4096]);

        public InflaterOutputStream inflaterOutputStream(@NonNull OutputStream dst, @NonNull Inflater inflater) {
            InflaterOutputStream instance = PUnsafe.allocateInstance(InflaterOutputStream.class);

            PUnsafe.putObject(instance, FOS_OUT_OFFSET, dst);
            PUnsafe.putObject(instance, IOS_INF_OFFSET, inflater);
            PUnsafe.putObject(instance, IOS_BUF_OFFSET, BUFFERS.get()[0]);

            return instance;
        }

        public DeflaterOutputStream deflaterOutputStream(@NonNull OutputStream dst, @NonNull Deflater deflater) {
            DeflaterOutputStream instance = PUnsafe.allocateInstance(DeflaterOutputStream.class);

            PUnsafe.putObject(instance, FOS_OUT_OFFSET, dst);
            PUnsafe.putObject(instance, DOS_DEF_OFFSET, deflater);
            PUnsafe.putObject(instance, DOS_BUF_OFFSET, BUFFERS.get()[1]);

            return instance;
        }
    }
}

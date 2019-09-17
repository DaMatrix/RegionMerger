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
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.binary.netty.NettyUtil;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

import static net.daporkchop.regionmerger.anvil.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class Optimize implements Mode {
    protected static final Option.Flag RECOMPRESS            = Option.flag("c");
    protected static final Option.Int  LEVEL                 = Option.integer("l", 7, 0, 8);
    protected static final Option.Int  PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);

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
            ThreadLocal<Inflater> inflaterCache = ThreadLocal.withInitial(Inflater::new);
            ThreadLocal<Deflater> deflaterCache = ThreadLocal.withInitial(() -> new Deflater(level));
            recoder = (src, dst) -> {
                Deflater deflater = deflaterCache.get();
                Inflater inflater = inflaterCache.get();

                int oldIndex = dst.writerIndex();
                dst.writeInt(-1).writeByte(ID_DEFLATE);
                if (src.readerIndex(4).readByte() != ID_DEFLATE) {
                    throw new IllegalStateException("Can't optimize GZIPped chunks!");
                }

                //try (OutputStream out = new InflaterOutputStream(new DeflaterOutputStream(NettyUtil.wrapOut(dst), deflaterCache.get()), inflaterCache.get())) {
                try (OutputStream out = FastStreams.inflaterOutputStream(FastStreams.deflaterOutputStream(NettyUtil.wrapOut(dst), deflater), inflater)) {
                    src.readBytes(out, src.readableBytes());
                    if (src.isReadable()) {
                        throw new IllegalStateException("Couldn't copy entire chunk into output buffer!");
                    }
                }
                dst.setInt(oldIndex, dst.writerIndex() - oldIndex - 4);

                deflater.reset();
                inflater.reset();
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
            ByteBuf src = null;
            ByteBuf dst = PooledByteBufAllocator.DEFAULT.ioBuffer(SECTOR_BYTES * (2 + 32 * 32)).writeBytes(EMPTY_HEADERS);
            try {
                try (FileChannel channel = FileChannel.open(file.toPath(), INPUT_OPEN_OPTIONS)) {
                    long size = channel.size();
                    if (size > Integer.MAX_VALUE) {
                        throw new IllegalStateException(String.format("Region \"%s\" too large: %d bytes", file, size));
                    }
                    src = PooledByteBufAllocator.DEFAULT.ioBuffer((int) size, (int) size);
                    int read = src.writeBytes(channel, 0L, (int) size);
                    if (read != size) {
                        throw new IllegalStateException(String.format("Only read %d/%d bytes!", read, size));
                    }
                }

                int sector = 2;
                int chunks = 0;
                for (int x = 31; x >= 0; x--) {
                    for (int z = 31; z >= 0; z--) {
                        final int chunkOffset = src.getInt(getOffsetIndex(x, z));
                        if (chunkOffset == 0) {
                            continue;
                        }

                        final int chunkPos = (chunkOffset >>> 8) * SECTOR_BYTES;
                        final int sizeBytes = src.getInt(chunkPos);

                        dst.setInt(getTimestampIndex(x, z), src.getInt(getTimestampIndex(x, z))); //copy timestamp

                        recoder.recode(src.slice(chunkPos, sizeBytes + 4), dst);
                        dst.writeBytes(EMPTY_SECTOR, 0, ((dst.writerIndex() - 1 >> 12) + 1 << 12) - dst.writerIndex()); //pad to next sector

                        final int chunkSectors = (dst.writerIndex() - 1 >> 12) + 1; //compute next chunk sector
                        dst.setInt(getOffsetIndex(x, z), (chunkSectors - sector) | (sector << 8)); //set offset value in region header
                        sector = chunkSectors;
                        chunks++;
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
                if (src != null) {
                    src.release();
                }
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

        public InflaterOutputStream inflaterOutputStream(@NonNull OutputStream dst, @NonNull Inflater inflater)    {
            InflaterOutputStream instance = PUnsafe.allocateInstance(InflaterOutputStream.class);

            PUnsafe.putObject(instance, FOS_OUT_OFFSET, dst);
            PUnsafe.putObject(instance, IOS_INF_OFFSET, inflater);
            PUnsafe.putObject(instance, IOS_BUF_OFFSET, BUFFERS.get()[0]);

            return instance;
        }

        public DeflaterOutputStream deflaterOutputStream(@NonNull OutputStream dst, @NonNull Deflater deflater)    {
            DeflaterOutputStream instance = PUnsafe.allocateInstance(DeflaterOutputStream.class);

            PUnsafe.putObject(instance, FOS_OUT_OFFSET, dst);
            PUnsafe.putObject(instance, DOS_DEF_OFFSET, deflater);
            PUnsafe.putObject(instance, DOS_BUF_OFFSET, BUFFERS.get()[1]);

            return instance;
        }
    }
}

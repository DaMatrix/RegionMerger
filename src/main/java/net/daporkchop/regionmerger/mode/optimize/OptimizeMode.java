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

package net.daporkchop.regionmerger.mode.optimize;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.logging.Logger;
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
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
public class OptimizeMode implements Mode {
    protected static final Option.Flag RECOMPRESS = Option.flag("c");
    protected static final Option.Flag USE_GZIP   = Option.flag("g");
    protected static final Option.Int  LEVEL      = Option.integer("l", 7, 0, 8);

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("Optimize:")
              .info("  Optimizes the size of a world by defragmenting and optionally re-compressing the regions.")
              .info("  This is only useful for worlds that will later be served read-only, as allowing the Minecraft client/server write access to an")
              .info("  optimized world will cause size to increase again.")
              .info("")
              .info("  Usage: optimize [options] <path>")
              .info("")
              .info("  Options:")
              .info("  -c          Enables re-compression of chunks. This will significantly increase the runtime (and CPU usage), but can help decrease")
              .info("              output size further.")
              .info("  -g          Use GZIP instead of DEFLATE for compression. This will increase runtime time, but may slightly reduce the size. Only")
              .info("              effective with -c.")
              .info("  -l <level>  Sets the level (intensity) of the compression, from 0-8. 0 is the worst, 8 is the best. Only effective with -c. Default: 7");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, false, RECOMPRESS, USE_GZIP, LEVEL);
    }

    @Override
    public String name() {
        return "optimize";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        boolean recompress = args.get(RECOMPRESS);
        boolean gzip = args.get(USE_GZIP);
        int level = args.get(LEVEL);
        World world = args.getDestination();

        Collection<File> regionsAsFiles = world.regions().stream().map(world::getAsFile).collect(Collectors.toList());

        long initialSize = regionsAsFiles.parallelStream().mapToLong(File::length).sum();
        logger.info("Initial size: %.2f MB", initialSize / (1024.0d * 1024.0d));

        regionsAsFiles.parallelStream().forEach((IOConsumer<File>) file -> {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(OverclockedRegionFile.SECTOR_BYTES * 3);
            try {
                int sector = 2;
                int chunks = 0;
                try (OverclockedRegionFile region = new OverclockedRegionFile(file, true, false)) {
                    for (int x = 31; x >= 0; x--) {
                        for (int z = 31; z >= 0; z--) {
                            ByteBuf chunk = region.readDirect(x, z);
                            if (chunk != null)  {
                                try {
                                    buf.writerIndex(OverclockedRegionFile.SECTOR_BYTES * sector);
                                    buf.writeBytes(chunk);
                                    if (chunk.isReadable()) {
                                        throw new IllegalStateException("Couldn't copy entire chunk into output buffer!");
                                    }
                                    int requiredSectors = chunk.readableBytes() / OverclockedRegionFile.SECTOR_BYTES + 1;
                                    buf.setInt((x << 2) | (z << 7), requiredSectors | (sector << 8));
                                    sector += requiredSectors;
                                    chunks++;
                                } finally {
                                    chunk.release();
                                }
                            }
                        }
                    }
                }
                if (chunks == 0 && !file.delete()) {
                    throw new IllegalStateException(String.format("Couldn't delete file \"%s\"!", file.getAbsolutePath()));
                } else {
                    try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                        int writeable = buf.readableBytes();
                        int written = buf.readBytes(channel, writeable);
                        if (writeable != written) {
                            throw new IllegalStateException(String.format("Only wrote %d/%d bytes!", written, writeable));
                        }
                    }
                }
            } finally {
                buf.release();
            }
        });

        int oldCount = regionsAsFiles.size();
        regionsAsFiles.removeIf(f -> !f.exists());
        long finalSize = regionsAsFiles.parallelStream().mapToLong(File::length).sum();
        logger.success("Done! Processed %d regions (deleting %d empty regions)", oldCount, oldCount - regionsAsFiles.size());
        logger.success("Shrunk by %.2f MB (%.3f%%)", (initialSize - finalSize) / (1024.0d * 1024.0d), (double) finalSize / (double) initialSize);
    }
}

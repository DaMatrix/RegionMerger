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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.function.throwing.ERunnable;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.util.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * this code is not good.
 *
 * @author DaPorkchop_
 */
public class MapMode implements Mode {
    protected static final Option<Integer> PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);
    protected static final Option<Type> TYPE = Option.ofEnum("-type", Type.class, null);
    protected static final Option<String> OUTPUT = Option.text("-output", "map.png");
    protected static final Option<Boolean> OVERWRITE = Option.flag("o");

    protected static final OpenOption[] READ_OPEN_OPTIONS = { StandardOpenOption.READ };

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  map:")
                .info("    Generates a map of the given input world based on some value.")
                .info("")
                .info("    Usage:")
                .info("      merge [options] <source>")
                .info("")
                .info("    Options:")
                .info("      --type <type>    Sets the type of map that will be created. Options: age, exists, size, size_fast")
                .info("      --output <file>  Sets the file that the map will be written to. Default: map.png")
                .info("      -o               Allows overwriting an existing output file.")
                .info("      -p <time>        Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(false, true, TYPE, OUTPUT, OVERWRITE, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "map";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final List<World> sources = args.getSources();

        if (sources.size() != 1) {
            logger.alert("Expected exactly 1 source world, but found %d!", sources.size());
            System.exit(1);
        }

        World world = sources.get(0);
        Collection<Vec2i> regionPositions = world.regions();

        logger.info("Loaded %d input worlds with a total of %d distinct regions.", sources.size(), regionPositions.size());

        final File imageFile = new File(args.get(OUTPUT));
        if (PFiles.checkFileExists(imageFile) && !args.get(OVERWRITE)) {
            throw new IllegalStateException(imageFile + " already exists (use -o to allow overwriting)");
        }

        final Type type = args.get(TYPE);

        IntSummaryStatistics xs = regionPositions.stream().mapToInt(Vec2i::getX).summaryStatistics();
        IntSummaryStatistics zs = regionPositions.stream().mapToInt(Vec2i::getY).summaryStatistics();
        final int minX = xs.getMin();
        final int minZ = zs.getMin();
        final int sizeX = xs.getMax() - xs.getMin() + 1;
        final int sizeZ = zs.getMax() - zs.getMin() + 1;

        logger.info("Creating %dx%d image buffer...", sizeX << 5, sizeZ << 5);
        BufferedImage image = type.createImage(sizeX << 5, sizeZ << 5);

        ThreadLocal<int[]> pixelBuffer = ThreadLocal.withInitial(() -> new int[32 * 32]);

        AtomicLong remainingRegions = new AtomicLong(regionPositions.size());

        {
            final int delay = args.get(PROGRESS_UPDATE_DELAY);
            if (delay > 0) {
                Thread t = new Thread((ERunnable) () -> {
                    Logger channel = logger.channel("Progress");
                    int total = regionPositions.size();
                    while (true) {
                        Thread.sleep(delay);
                        long remaining = remainingRegions.get();
                        channel.info("Processed %d/%d regions (%.3f%%)", total - remaining, total, (float) (total - remaining) / (float) total * 100.0f);
                        if (remaining == 0) {
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }
        }

        regionPositions.stream().parallel()
                .forEach((IOConsumer<Vec2i>) pos -> {
                    ByteBuf headers = null;
                    try {
                        try (FileChannel channel = FileChannel.open(world.getAsFile(pos).toPath(), READ_OPEN_OPTIONS)) {
                            int size = Math.min(type.maxDataSize, toInt(channel.size()));
                            headers = ByteBufAllocator.DEFAULT.ioBuffer(size, size);
                            do {
                                int writerIndex = headers.writerIndex();
                                headers.writeBytes(channel, writerIndex, size - writerIndex);
                            } while (headers.readableBytes() < size);
                        }
                        type.region(image, headers, pos.getX(), pos.getY(), (pos.getX() - minX) << 5, (pos.getY() - minZ) << 5, pixelBuffer.get());
                    } finally {
                        ReferenceCountUtil.release(headers);
                    }
                    remainingRegions.getAndDecrement();
                });

        logger.info("Writing image...");
        type.finish(image);
        ImageIO.write(image, "png", imageFile);
    }

    @RequiredArgsConstructor
    enum Type {
        AGE(8192) {
            int[] values;
            int sizeX;
            int sizeZ;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                Arrays.fill(this.values = new int[sizeX * sizeZ], -1);
                this.sizeX = sizeX;
                this.sizeZ = sizeZ;
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++) {
                        if (buffer.getInt(getOffsetIndex(x, z)) != 0) {
                            this.values[(pz + z) * this.sizeX + (px + x)] = buffer.getInt(getTimestampIndex(x, z));
                        }
                    }
                }
            }

            @Override
            void finish(BufferedImage image) {
                IntSummaryStatistics stats = Arrays.stream(this.values).filter(i -> i >= 0).summaryStatistics();
                int min = stats.getMin();
                int max = stats.getMax();

                for (int i = 0; i < this.values.length; i++) {
                    int color = 0xFF000000;
                    int value = this.values[i];
                    if (value >= 0) {
                        int v = clamp(floorI((double) (value - min) * 511.0d / (max - min)), 0, 511);
                        color |= (clamp(511 - v, 0, 255) << 16) | (clamp(v, 0, 255) << 8);
                    }
                    this.values[i] = color;
                }

                image.setRGB(0, 0, this.sizeX, this.sizeZ, this.values, 0, this.sizeX);
            }
        },
        EXISTS(4096) {
            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_BYTE_BINARY);
            }

            @Override
            void region(BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int x = 0; x < 32; x++) {
                    for (int z = 0; z < 32; z++) {
                        pixelBuffer[z * 32 + x] = buffer.getInt(getOffsetIndex(x, z)) == 0 ? 0xFF000000 : 0xFFFFFFFF;
                    }
                }

                synchronized (image) {
                    image.setRGB(px, pz, 32, 32, pixelBuffer, 0, 32);
                }
            }
        },
        SIZE(Integer.MAX_VALUE) {
            int[] values;
            int sizeX;
            int sizeZ;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                Arrays.fill(this.values = new int[sizeX * sizeZ], -1);
                this.sizeX = sizeX;
                this.sizeZ = sizeZ;
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++) {
                        int offset = buffer.getInt(getOffsetIndex(x, z));
                        if (offset != 0) {
                            this.values[(pz + z) * this.sizeX + (px + x)] = buffer.getInt((offset >> 8) * SECTOR_BYTES);
                        }
                    }
                }
            }

            @Override
            void finish(BufferedImage image) {
                IntSummaryStatistics stats = Arrays.stream(this.values).filter(i -> i >= 0).summaryStatistics();
                int min = stats.getMin();
                int max = stats.getMax();

                for (int i = 0; i < this.values.length; i++) {
                    int color = 0xFF000000;
                    int value = this.values[i];
                    if (value >= 0) {
                        int v = clamp(floorI((double) (value - min) * 511.0d / (max - min)), 0, 511);
                        color |= (clamp(511 - v, 0, 255) << 16) | (clamp(v, 0, 255) << 8);
                    }
                    this.values[i] = color;
                }

                image.setRGB(0, 0, this.sizeX, this.sizeZ, this.values, 0, this.sizeX);
            }
        },
        SIZE_FAST(4096) {
            int[] values;
            int sizeX;
            int sizeZ;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                Arrays.fill(this.values = new int[sizeX * sizeZ], -1);
                this.sizeX = sizeX;
                this.sizeZ = sizeZ;
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++) {
                        int offset = buffer.getInt(getOffsetIndex(x, z));
                        if (offset != 0) {
                            this.values[(pz + z) * this.sizeX + (px + x)] = (offset & 0xFF) * SECTOR_BYTES;
                        }
                    }
                }
            }

            @Override
            void finish(BufferedImage image) {
                IntSummaryStatistics stats = Arrays.stream(this.values).filter(i -> i >= 0).summaryStatistics();
                int min = stats.getMin();
                int max = stats.getMax();

                for (int i = 0; i < this.values.length; i++) {
                    int color = 0xFF000000;
                    int value = this.values[i];
                    if (value >= 0) {
                        int v = clamp(floorI((double) (value - min) * 511.0d / (max - min)), 0, 511);
                        color |= (clamp(511 - v, 0, 255) << 16) | (clamp(v, 0, 255) << 8);
                    }
                    this.values[i] = color;
                }

                image.setRGB(0, 0, this.sizeX, this.sizeZ, this.values, 0, this.sizeX);
            }
        };

        private final int maxDataSize;

        abstract BufferedImage createImage(int sizeX, int sizeZ);

        abstract void region(BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException;

        void finish(BufferedImage image) {
        }
    }
}

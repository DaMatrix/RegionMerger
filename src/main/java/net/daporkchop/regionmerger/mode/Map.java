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
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;

import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * this code is not good.
 *
 * @author DaPorkchop_
 */
public class Map implements Mode {
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
                .info("      --type <type>       Sets the type of map that will be created. Options: age, exists, size, size_fast")
                .info("      --output <file>     Sets the file that the map will be written to. Default: map.png")
                .info("      -o                  Allows overwriting an existing output file.");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(false, true, TYPE, OUTPUT, OVERWRITE);
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

        BufferedImage image = type.createImage(sizeX << 5, sizeZ << 5);

        ThreadLocal<int[]> pixelBuffer = ThreadLocal.withInitial(() -> new int[32 * 32]);

        for (int pass = 0; pass < type.passes; pass++) {
            int _pass = pass;
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
                            type.region(_pass, image, headers, pos.getX(), pos.getY(), (pos.getX() - minX) << 5, (pos.getY() - minZ) << 5, pixelBuffer.get());
                        } finally {
                            ReferenceCountUtil.release(headers);
                        }
                    });
            logger.info("Completed pass #%d", pass);
        }

        logger.info("Writing image...");
        ImageIO.write(image, "png", imageFile);
    }

    @RequiredArgsConstructor
    enum Type {
        AGE(2, 8192) {
            int minAge = Integer.MAX_VALUE;
            int maxAge = Integer.MIN_VALUE;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(int pass, BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                switch (pass) {
                    case 0: {
                        int minAge = Integer.MAX_VALUE;
                        int maxAge = Integer.MIN_VALUE;

                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                if (buffer.getInt(getOffsetIndex(x, z)) != 0) {
                                    int age = buffer.getInt(getTimestampIndex(x, z));
                                    minAge = Math.min(minAge, age);
                                    maxAge = Math.max(maxAge, age);
                                }
                            }
                        }

                        synchronized (this) {
                            this.minAge = Math.min(this.minAge, minAge);
                            this.maxAge = Math.max(this.maxAge, maxAge);
                        }
                        break;
                    }
                    case 1: {
                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int color = 0xFF000000;
                                if (buffer.getInt(getOffsetIndex(x, z)) != 0) {
                                    int age = buffer.getInt(getTimestampIndex(x, z));
                                    int i = clamp(floorI((double) (age - this.minAge) * 511.0d / (this.maxAge - this.minAge)), 0, 511);
                                    color |= (clamp(511 - i, 0, 255) << 16) | (clamp(i, 0, 255) << 8);
                                }
                                pixelBuffer[z * 32 + x] = color;
                            }
                        }

                        synchronized (image) {
                            image.setRGB(px, pz, 32, 32, pixelBuffer, 0, 32);
                        }
                        break;
                    }
                }
            }
        },
        EXISTS(1, 4096) {
            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_BYTE_BINARY);
            }

            @Override
            void region(int pass, BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
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
        SIZE(2, Integer.MAX_VALUE) {
            int minSize = Integer.MAX_VALUE;
            int maxSize = Integer.MIN_VALUE;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(int pass, BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                switch (pass) {
                    case 0: {
                        int minSize = Integer.MAX_VALUE;
                        int maxSize = Integer.MIN_VALUE;

                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int offset = buffer.getInt(getOffsetIndex(x, z));
                                if (offset != 0) {
                                    int size = buffer.getInt((offset >> 8) * SECTOR_BYTES);
                                    minSize = Math.min(minSize, size);
                                    maxSize = Math.max(maxSize, size);
                                }
                            }
                        }

                        synchronized (this) {
                            this.minSize = Math.min(this.minSize, minSize);
                            this.maxSize = Math.max(this.maxSize, maxSize);
                        }
                        break;
                    }
                    case 1: {
                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int color = 0xFF000000;

                                int offset = buffer.getInt(getOffsetIndex(x, z));
                                if (offset != 0) {
                                    int size = buffer.getInt((offset >> 8) * SECTOR_BYTES);
                                    int i = clamp(floorI((double) (size - this.minSize) * 511.0d / (this.maxSize - this.minSize)), 0, 511);
                                    color |= (clamp(511 - i, 0, 255) << 16) | (clamp(i, 0, 255) << 8);
                                }
                                pixelBuffer[z * 32 + x] = color;
                            }
                        }

                        synchronized (image) {
                            image.setRGB(px, pz, 32, 32, pixelBuffer, 0, 32);
                        }
                        break;
                    }
                }
            }
        },
        SIZE_FAST(2, 4096) {
            int minSize = Integer.MAX_VALUE;
            int maxSize = Integer.MIN_VALUE;

            @Override
            BufferedImage createImage(int sizeX, int sizeZ) {
                return new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_RGB);
            }

            @Override
            void region(int pass, BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                switch (pass) {
                    case 0: {
                        int minSize = Integer.MAX_VALUE;
                        int maxSize = Integer.MIN_VALUE;

                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int offset = buffer.getInt(getOffsetIndex(x, z));
                                if (offset != 0) {
                                    int size = (offset & 0xFF) * SECTOR_BYTES;
                                    minSize = Math.min(minSize, size);
                                    maxSize = Math.max(maxSize, size);
                                }
                            }
                        }

                        synchronized (this) {
                            this.minSize = Math.min(this.minSize, minSize);
                            this.maxSize = Math.max(this.maxSize, maxSize);
                        }
                        break;
                    }
                    case 1: {
                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int color = 0xFF000000;

                                int offset = buffer.getInt(getOffsetIndex(x, z));
                                if (offset != 0) {
                                    int size = (offset & 0xFF) * SECTOR_BYTES;
                                    int i = clamp(floorI((double) (size - this.minSize) * 511.0d / (this.maxSize - this.minSize)), 0, 511);
                                    color |= (clamp(511 - i, 0, 255) << 16) | (clamp(i, 0, 255) << 8);
                                }
                                pixelBuffer[z * 32 + x] = color;
                            }
                        }

                        synchronized (image) {
                            image.setRGB(px, pz, 32, 32, pixelBuffer, 0, 32);
                        }
                        break;
                    }
                }
            }
        };

        private final int passes;
        private final int maxDataSize;

        abstract BufferedImage createImage(int sizeX, int sizeZ);

        abstract void region(int pass, BufferedImage image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException;
    }
}

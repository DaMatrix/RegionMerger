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
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;
import net.daporkchop.regionmerger.util.World;
import org.gdal.gdal.Band;
import org.gdal.gdal.ColorTable;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.TermProgressCallback;
import org.gdal.gdal.gdal;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
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
import java.util.stream.IntStream;

import static java.lang.Math.*;
import static net.daporkchop.lib.common.math.PMath.*;
import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;
import static org.gdal.gdalconst.gdalconstConstants.*;

/**
 * this code is not good.
 *
 * @author DaPorkchop_
 */
public class MapMode implements Mode {
    protected static final Option<Integer> PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);
    protected static final Option<Type> TYPE = Option.ofEnum("-type", Type.class, null);
    protected static final Option<Format> FORMAT = Option.ofEnum("-format", Format.class, Format.PNG);
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
                .info("      --type <type>      Sets the type of map that will be created. Options: age, exists, size, size_fast")
                .info("      --output <file>    Sets the file that the map will be written to. Default: map.png")
                .info("      --format <format>  Sets the image format that will be used. Default: png, Options: png, geotiff")
                .info("      -o                 Allows overwriting an existing output file.")
                .info("      -p <time>          Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(false, true, TYPE, FORMAT, OUTPUT, OVERWRITE, PROGRESS_UPDATE_DELAY);
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
        PFiles.rm(imageFile);

        final Type type = args.get(TYPE);
        final Format format = args.get(FORMAT);

        IntSummaryStatistics xs = regionPositions.stream().mapToInt(Vec2i::getX).summaryStatistics();
        IntSummaryStatistics zs = regionPositions.stream().mapToInt(Vec2i::getY).summaryStatistics();
        final int minX = xs.getMin();
        final int minZ = zs.getMin();
        final int sizeX = xs.getMax() - xs.getMin() + 1;
        final int sizeZ = zs.getMax() - zs.getMin() + 1;

        Image image = type.createImage(format, minX, minZ, sizeX, sizeZ, imageFile);
        ThreadLocal<int[]> pixelBuffer = ThreadLocal.withInitial(() -> new int[32 * 32]);

        logger.info("Starting...");

        AtomicLong remainingRegions = new AtomicLong(regionPositions.size());

        Thread notifierThread = null;
        {
            final int delay = args.get(PROGRESS_UPDATE_DELAY);
            if (delay > 0) {
                notifierThread = new Thread(() -> {
                    Logger channel = logger.channel("Progress");
                    int total = regionPositions.size();
                    while (true) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        long remaining = remainingRegions.get();
                        channel.info("Processed %d/%d regions (%.3f%%)", total - remaining, total, (float) (total - remaining) / (float) total * 100.0f);
                        if (remaining == 0 || Thread.interrupted()) {
                            return;
                        }
                    }
                });
                notifierThread.setDaemon(true);
                notifierThread.start();
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

        if (notifierThread != null) {
            try {
                notifierThread.interrupt();
                notifierThread.join();
            } catch (Exception e) {
                logger.alert(e);
            }
        }

        type.finish(image);
        image.close();
    }

    @RequiredArgsConstructor
    enum Type {
        AGE(8192) {
            @Override
            Image createImage(@NonNull Format format, int minX, int maxZ, int sizeX, int sizeZ, @NonNull File dst) throws IOException {
                return format.createImage(ImageType.INT_SCALE, minX << 5, maxZ << 5, sizeX << 5, sizeZ << 5, 16, dst);
            }

            @Override
            void region(Image image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int i = 0, z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++, i++) {
                        pixelBuffer[i] = buffer.getInt(getOffsetIndex(x, z)) != 0 ? buffer.getInt(getTimestampIndex(x, z)) : -1;
                    }
                }
                image.set(rx << 5, rz << 5, 32, 32, pixelBuffer);
            }
        },
        EXISTS(4096) {
            @Override
            Image createImage(@NonNull Format format, int minX, int maxZ, int sizeX, int sizeZ, @NonNull File dst) throws IOException {
                return format.createImage(ImageType.BOOLEAN, minX << 5, maxZ << 5, sizeX << 5, sizeZ << 5, 16, dst);
            }

            @Override
            void region(Image image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int i = 0, z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++, i++) {
                        pixelBuffer[i] = buffer.getInt(getOffsetIndex(x, z)) == 0 ? 0xFF000000 : 0xFFFFFFFF;
                    }
                }
                image.set(rx << 5, rz << 5, 32, 32, pixelBuffer);
            }
        },
        SIZE(Integer.MAX_VALUE) {
            @Override
            Image createImage(@NonNull Format format, int minX, int maxZ, int sizeX, int sizeZ, @NonNull File dst) throws IOException {
                return format.createImage(ImageType.INT_SCALE, minX << 5, maxZ << 5, sizeX << 5, sizeZ << 5, 16, dst);
            }

            @Override
            void region(Image image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int i = 0, z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++, i++) {
                        int offset = buffer.getInt(getOffsetIndex(x, z));
                        pixelBuffer[i] = offset != 0 ? buffer.getInt((offset >> 8) * SECTOR_BYTES) : -1;
                    }
                }
                image.set(rx << 5, rz << 5, 32, 32, pixelBuffer);
            }
        },
        SIZE_FAST(4096) {
            @Override
            Image createImage(@NonNull Format format, int minX, int maxZ, int sizeX, int sizeZ, @NonNull File dst) throws IOException {
                return format.createImage(ImageType.INT_SCALE, minX << 5, maxZ << 5, sizeX << 5, sizeZ << 5, 16, dst);
            }

            @Override
            void region(Image image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException {
                for (int i = 0, z = 0; z < 32; z++) {
                    for (int x = 0; x < 32; x++, i++) {
                        int offset = buffer.getInt(getOffsetIndex(x, z));
                        pixelBuffer[i] = offset != 0 ? (offset & 0xFF) * SECTOR_BYTES : -1;
                    }
                }
                image.set(rx << 5, rz << 5, 32, 32, pixelBuffer);
            }
        };

        private final int maxDataSize;

        abstract Image createImage(@NonNull Format format, int minX, int maxZ, int sizeX, int sizeZ, @NonNull File dst) throws IOException;

        abstract void region(Image image, ByteBuf buffer, int rx, int rz, int px, int pz, int[] pixelBuffer) throws IOException;

        void finish(Image image) {
        }
    }

    enum ImageType {
        BOOLEAN,
        BOOLEAN_WITH_TRANSPARENCY,
        INT_SCALE;
    }

    @RequiredArgsConstructor
    enum Format {
        PNG("png") {
            @Override
            Image createImage(@NonNull ImageType type, int minX, int minZ, int sizeX, int sizeZ, int scale, @NonNull File dst) throws IOException {
                logger.info("Creating %dx%d image buffer...", sizeX, sizeZ);

                switch (type) {
                    case BOOLEAN:
                        return new Image() {
                            final BufferedImage image = new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_BYTE_BINARY);

                            @Override
                            public synchronized void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                this.image.setRGB(x - minX, z - minZ, sizeX, sizeZ, data, 0, sizeX);
                            }

                            @Override
                            public void close() throws IOException {
                                logger.info("Writing image...");
                                ImageIO.write(this.image, "png", dst);
                            }
                        };
                    case BOOLEAN_WITH_TRANSPARENCY:
                        return new Image() {
                            final byte[] arr = { 0, (byte) 0xFF };
                            final BufferedImage image = new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_BYTE_INDEXED, new IndexColorModel(2, 2, this.arr, this.arr, this.arr, 2));

                            @Override
                            public synchronized void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                this.image.setRGB(x - minX, z - minZ, sizeX, sizeZ, data, 0, sizeX);
                            }

                            @Override
                            public void close() throws IOException {
                                logger.info("Writing image...");
                                ImageIO.write(this.image, "png", dst);
                            }
                        };
                    case INT_SCALE:
                        return new Image() {
                            final int _sizeX = sizeX;
                            final int[] arr = new int[sizeX * sizeZ];

                            {
                                Arrays.fill(this.arr, -1);
                            }

                            @Override
                            public void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                for (int dz = 0; dz < sizeZ; dz++) {
                                    System.arraycopy(data, dz * sizeX, this.arr, (z - minZ + dz) * this._sizeX + (x - minX), sizeX);
                                }
                            }

                            @Override
                            public void close() throws IOException {
                                logger.info("scaling colors...");
                                IntSummaryStatistics stats = Arrays.stream(this.arr).filter(i -> i >= 0).summaryStatistics();
                                int min = stats.getMin();
                                int max = stats.getMax();

                                for (int i = 0; i < this.arr.length; i++) {
                                    int color = 0;
                                    int value = this.arr[i];
                                    if (value >= 0) {
                                        int v = clamp(floorI((double) (value - min) * 511.0d / (max - min)), 0, 511);
                                        color = 0xFF000000 | ((clamp(511 - v, 0, 255) << 16) | (clamp(v, 0, 255) << 8));
                                    }
                                    this.arr[i] = color;
                                }

                                logger.info("Writing image...");
                                BufferedImage image = new BufferedImage(sizeX, sizeZ, BufferedImage.TYPE_INT_ARGB);
                                image.setRGB(0, 0, sizeX, sizeZ, this.arr, 0, sizeX);
                                ImageIO.write(image, "png", dst);
                            }
                        };
                }
                throw new IllegalArgumentException(type.name());
            }
        },
        GEOTIFF("tiff") {
            @Override
            Image createImage(@NonNull ImageType type, int minX, int minZ, int sizeX, int sizeZ, int scale, @NonNull File dst) throws IOException {
                logger.info("Creating %dx%d GeoTIFF...", sizeX, sizeZ);

                gdal.AllRegister();

                int tileSize = 1024;
                String[] options = {
                        "TILED=YES",
                        "NBITS=" + (type == ImageType.BOOLEAN ? "1" : type == ImageType.BOOLEAN_WITH_TRANSPARENCY ? "2" : "32"),
                        "DISCARD_LSB=" + (type == ImageType.INT_SCALE ? System.getProperty("discardBits", "0") : "0"),
                        "COMPRESS=DEFLATE",
                        "ZLEVEL=9",
                        "NUM_THREADS=ALL_CPUS",
                        "PREDICTOR=" + (type == ImageType.INT_SCALE ? "2" : "1"),
                        "BLOCKXSIZE=" + tileSize,
                        "BLOCKYSIZE=" + tileSize,
                        "SPARSE_OK=TRUE",
                        "BIGTIFF=YES"
                };
                gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS");
                gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE");
                gdal.SetConfigOption("PREDICTOR_OVERVIEW", type == ImageType.INT_SCALE ? "2" : "1");

                final Dataset dataset = gdal.GetDriverByName("GTiff").Create(
                        dst.getAbsolutePath(),
                        sizeX, sizeZ,
                        1, type == ImageType.INT_SCALE ? GDT_Int32 : GDT_Byte,
                        options);
                dataset.SetGeoTransform(new double[]{
                        minX * scale, scale, 0,
                        (sizeZ - (minZ + sizeZ)) * scale, 0, -scale
                });
                Band band = dataset.GetRasterBand(1);

                Runnable finish = () -> {
                    logger.info("Flushing image...");
                    dataset.FlushCache();

                    int[] overviews = IntStream.range(1, 32).filter(i -> (i & 3) == 0 && (max(sizeX, sizeZ) >> i) >= tileSize).map(i -> 1 << i).toArray();
                    logger.info("Generating %d overviews...", overviews.length);
                    if (overviews.length != 0) {
                        dataset.BuildOverviews("AVERAGE", overviews, new TermProgressCallback());
                    }

                    logger.info("Finishing image...");
                    dataset.delete();
                };

                switch (type) {
                    case BOOLEAN:
                        return new Image() {
                            @Override
                            public synchronized void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                for (int i = 0, lim = sizeX * sizeZ; i < lim; i++) {
                                    int val = data[i];
                                    data[i] = val == 0 ? 0 : 1;
                                }

                                synchronized (this) {
                                    band.WriteRaster(x - minX, z - minZ, sizeX, sizeZ, data);
                                }
                            }

                            @Override
                            public void close() throws IOException {
                                finish.run();
                            }
                        };
                    case BOOLEAN_WITH_TRANSPARENCY:
                        return new Image() {
                            {
                                band.SetNoDataValue(0);
                                ColorTable colorTable = new ColorTable();
                                colorTable.SetColorEntry(0, new Color(0, true));
                                colorTable.SetColorEntry(1, Color.BLACK);
                                colorTable.SetColorEntry(2, Color.WHITE);
                                band.SetColorTable(colorTable);
                            }

                            @Override
                            public void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                for (int i = 0, lim = sizeX * sizeZ; i < lim; i++) {
                                    int val = data[i];
                                    data[i] = val < 0 ? 0 : val == 0 ? 1 : 2;
                                }

                                synchronized (this) {
                                    band.WriteRaster(x - minX, z - minZ, sizeX, sizeZ, data);
                                }
                            }

                            @Override
                            public void close() throws IOException {
                                finish.run();
                            }
                        };
                    case INT_SCALE:
                        return new Image() {
                            final int nodata = -1 & -(1 << Integer.parseUnsignedInt(System.getProperty("discardBits", "0")) >> 1);

                            {
                                band.SetNoDataValue(this.nodata);
                                band.Fill(this.nodata);
                            }

                            @Override
                            public synchronized void set(int x, int z, int sizeX, int sizeZ, int[] data) {
                                for (int i = 0, lim = sizeX * sizeZ; i < lim; i++) {
                                    int val = data[i];
                                    data[i] = val < 0 ? this.nodata : val;
                                }

                                synchronized (this) {
                                    band.WriteRaster(x - minX, z - minZ, sizeX, sizeZ, data);
                                }
                            }

                            @Override
                            public void close() throws IOException {
                                finish.run();
                            }
                        };
                }

                throw new IllegalArgumentException(type.name());
            }
        };

        @NonNull
        final String extension;

        abstract Image createImage(@NonNull ImageType type, int minX, int minZ, int sizeX, int sizeZ, int scale, @NonNull File dst) throws IOException;
    }

    interface Image extends AutoCloseable {
        void set(int x, int z, int sizeX, int sizeZ, int[] data);

        @Override
        void close() throws IOException;
    }
}

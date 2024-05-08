/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2018-2024 DaPorkchop_
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
import net.daporkchop.lib.common.pool.handle.Handle;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;
import net.daporkchop.regionmerger.util.World;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * this code is not good.
 *
 * @author DaPorkchop_
 */
public class Dump implements Mode {
    protected static final Option<Integer> PROGRESS_UPDATE_DELAY = Option.integer("p", 5000, 0, Integer.MAX_VALUE);
    protected static final Option<Type> TYPE = Option.ofEnum("-type", Type.class, null);
    protected static final Option<String> OUTPUT = Option.text("-output", "timestamps.csv");
    protected static final Option<Boolean> OVERWRITE = Option.flag("o");

    protected static final OpenOption[] READ_OPEN_OPTIONS = { StandardOpenOption.READ };

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("  dump:")
                .info("    Dumps all of the chunk timestamps from all of the provided source worlds into a .")
                .info("")
                .info("    Usage:")
                .info("      dump [options] <source> [source]...")
                .info("")
                .info("    Options:")
                .info("      --type <type>      Sets the type of data that will be dumped. Options: timestamp, size, size_fast")
                .info("      --output <file>    Sets the file that the map will be written to. Default: <type>.csv")
                .info("      -o                 Allows overwriting an existing output file.")
                .info("      -p <time>          Sets the time (in ms) between progress updates. Set to 0 to disable. Default: 5000");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(false, true, TYPE, OUTPUT, OVERWRITE, PROGRESS_UPDATE_DELAY);
    }

    @Override
    public String name() {
        return "dump";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final List<World> sources = args.getSources();

        if (sources.isEmpty()) {
            logger.alert("Expected at least 1 source world!");
            System.exit(1);
        }

        long totalRegions = sources.stream().map(World::regions).mapToInt(Collection::size).sum();
        long distinctRegions = sources.stream().map(World::regions).flatMap(Collection::stream).distinct().count();

        logger.info("Loaded %d input worlds with a total of %d regions (%d distinct regions).", sources.size(), totalRegions, distinctRegions);

        final File outputFile = new File(args.get(OUTPUT));
        if (PFiles.checkFileExists(outputFile) && !args.get(OVERWRITE)) {
            throw new IllegalStateException(outputFile + " already exists (use -o to allow overwriting)");
        }
        PFiles.rm(outputFile);

        final Type type = args.get(TYPE);

        logger.info("Starting...");

        AtomicLong remainingRegions = new AtomicLong(totalRegions);

        Thread notifierThread = null;
        {
            final int delay = args.get(PROGRESS_UPDATE_DELAY);
            if (delay > 0) {
                notifierThread = new Thread(() -> {
                    Logger channel = logger.channel("Progress");
                    while (true) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        long remaining = remainingRegions.get();
                        channel.info("Processed %d/%d regions (%.3f%%)", totalRegions - remaining, totalRegions, (float) (totalRegions - remaining) / (float) totalRegions * 100.0f);
                        if (remaining == 0L || Thread.interrupted()) {
                            return;
                        }
                    }
                });
                notifierThread.setDaemon(true);
                notifierThread.start();
            }
        }

        try (OutputStream out = Files.newOutputStream(outputFile.toPath())) {
            out.write(("chunkX,chunkZ," + type.name().toLowerCase(Locale.ROOT) + '\n').getBytes(StandardCharsets.US_ASCII));

            sources.parallelStream().forEach(world -> world.regions().parallelStream().forEach((IOConsumer<Vec2i>) pos -> {
                ByteBuf headers = null;
                try {
                    try (FileChannel channel = FileChannel.open(world.getAsFile(pos).toPath(), READ_OPEN_OPTIONS)) {
                        int size = Math.min(type.maxDataSize, Math.toIntExact(channel.size()));
                        headers = ByteBufAllocator.DEFAULT.ioBuffer(size, size);
                        do {
                            int writerIndex = headers.writerIndex();
                            headers.writeBytes(channel, writerIndex, size - writerIndex);
                        } while (headers.readableBytes() < size);
                    }

                    try (Handle<StringBuilder> handle = PorkUtil.STRINGBUILDER_POOL.get()) {
                        StringBuilder builder = handle.get();
                        builder.setLength(0);

                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                if (headers.getInt(getOffsetIndex(x, z)) != 0) {
                                    builder.append((pos.getX() << 5) | x).append(',').append((pos.getY() << 5) | z).append(',')
                                            .append(type.chunk(headers, pos.getX(), pos.getY(), x, z)).append('\n');
                                }
                            }
                        }

                        if (builder.length() != 0) {
                            byte[] data = builder.toString().getBytes(StandardCharsets.US_ASCII);
                            synchronized (out) {
                                out.write(data);
                            }
                        }
                    }
                } finally {
                    ReferenceCountUtil.release(headers);
                }
                remainingRegions.getAndDecrement();
            }));
        }

        if (notifierThread != null) {
            try {
                notifierThread.interrupt();
                notifierThread.join();
            } catch (Exception e) {
                logger.alert(e);
            }
        }
    }

    @RequiredArgsConstructor
    enum Type {
        AGE(8192) {
            @Override
            String chunk(ByteBuf buffer, int rx, int rz, int x, int z) {
                return String.valueOf(buffer.getInt(getTimestampIndex(x, z)));
            }
        },
        SIZE(Integer.MAX_VALUE) {
            @Override
            String chunk(ByteBuf buffer, int rx, int rz, int x, int z) {
                int offset = buffer.getInt(getOffsetIndex(x, z));
                return String.valueOf(buffer.getInt((offset >> 8) * SECTOR_BYTES));
            }
        },
        SIZE_FAST(4096) {
            @Override
            String chunk(ByteBuf buffer, int rx, int rz, int x, int z) {
                int offset = buffer.getInt(getOffsetIndex(x, z));
                return String.valueOf((offset & 0xFF) * SECTOR_BYTES);
            }
        };

        private final int maxDataSize;

        abstract String chunk(ByteBuf buffer, int rx, int rz, int x, int z);
    }
}

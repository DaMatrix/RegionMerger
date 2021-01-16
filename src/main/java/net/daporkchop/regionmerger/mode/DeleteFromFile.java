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

import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.lib.unsafe.PUnsafe;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;
import net.daporkchop.regionmerger.util.World;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static net.daporkchop.lib.common.util.PValidation.*;
import static net.daporkchop.lib.logging.Logging.*;
import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * @author DaPorkchop_
 */
public class DeleteFromFile implements Mode {
    protected static final Option<String> FILE = Option.text("-file", null);
    protected static final OpenOption[] DELETE_OPEN_OPTIONS = { StandardOpenOption.READ, StandardOpenOption.WRITE };

    @Override
    public void printUsage(@NonNull Logger logger) {
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, false, FILE);
    }

    @Override
    public String name() {
        return "deletefromfile";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final World dst = args.getDestination();

        logger.info("Loaded output world with %d existing regions.", dst.regions().size());

        String fileName = args.get(FILE);
        if (fileName == null) {
            logger.error("--file must be set!");
            System.exit(1);
        }

        Map<Vec2i, List<Integer>> missing;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName))) {
            String first = reader.readLine();
            checkArg("x,z".equals(first) || "z,x".equals(first), first);

            Matcher matcher = Pattern.compile(String.format("^(?<%s>-?\\d+),(?<%s>-?\\d+)$", first.charAt(0), first.charAt(2))).matcher("");
            missing = reader.lines()
                    .map(s -> {
                        checkArg(matcher.reset(s).matches(), "invalid line: %s", s);
                        return new Vec2i(Integer.parseInt(matcher.group("x")), Integer.parseInt(matcher.group("z")));
                    })
                    .distinct()
                    .collect(Collectors.groupingBy(
                            pos -> new Vec2i(pos.getX() >> 5, pos.getY() >> 5),
                            Collectors.mapping(pos -> getOffsetIndex(pos.getX() & 0x1F, pos.getY() & 0x1F), Collectors.toList())));
        }

        logger.info("Loaded %d missing chunk positions in %d regions.", missing.values().stream().mapToInt(List::size).sum(), missing.size());

        missing.entrySet().stream().parallel()
                .filter(entry -> dst.regions().contains(entry.getKey()))
                .forEach((IOConsumer<Map.Entry<Vec2i, List<Integer>>>) entry -> {
                    File regionFile = dst.getAsFile(entry.getKey());

                    boolean delete;
                    try (FileChannel channel = FileChannel.open(regionFile.toPath(), DELETE_OPEN_OPTIONS)) {
                        MappedByteBuffer headers = channel.map(FileChannel.MapMode.READ_WRITE, 0L, SECTOR_BYTES);
                        entry.getValue().forEach(i -> headers.putInt(i, 0));

                        //determine whether or not we should delete the file
                        int count = 0;
                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                if (headers.getInt(getOffsetIndex(x, z)) != 0) {
                                    count++;
                                }
                            }
                        }
                        delete = count == 0;

                        PUnsafe.pork_releaseBuffer(headers.force());
                    }

                    if (delete) {
                        PFiles.rm(regionFile);
                    }
                });

        logger.success("Done!");
    }
}

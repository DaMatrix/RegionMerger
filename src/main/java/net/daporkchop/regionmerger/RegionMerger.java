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
 *
 */

package net.daporkchop.regionmerger;

import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.mcworldlib.format.anvil.region.RegionConstants;
import net.daporkchop.regionmerger.mode.Add;
import net.daporkchop.regionmerger.mode.DeleteFromFile;
import net.daporkchop.regionmerger.mode.FindMissing;
import net.daporkchop.regionmerger.mode.Merge;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.mode.Optimize;
import net.daporkchop.regionmerger.option.Arguments;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static net.daporkchop.lib.logging.Logging.*;

/**
 * @author DaPorkchop_
 */
public class RegionMerger {
    public static final byte[] EMPTY_SECTOR = new byte[RegionConstants.SECTOR_BYTES];
    public static final byte[] EMPTY_HEADERS = new byte[RegionConstants.HEADER_BYTES];

    public static final Map<String, Mode> MODES = new HashMap<String, Mode>() {
        {
            this.put("add", new Add());
            this.put("findmissing", new FindMissing());
            this.put("merge", new Merge());
            this.put("optimize", new Optimize());

            this.put("debug-deletefromfile", new DeleteFromFile());
        }
    };

    public static void main(String... args) throws IOException {
        logger.enableANSI().redirectStdOut().addFile(new File("./regionmerger.log"), true, LogAmount.DEBUG);

        Thread.setDefaultUncaughtExceptionHandler((thread, e) -> {
            logger.channel(thread.getName()).alert(e);
            System.exit(1);
        });

        if (args.length == 0 || (args.length == 1 && ("--help".equals(args[0]) ||
                "-help".equals(args[0]) ||
                "-h".equals(args[0]) ||
                "--h".equals(args[0]) ||
                "help".equals(args[0])
        ))) {
            Logger log = logger.channel("Help");
            log.info("RegionMerger v0.1.9")
                    .info("  Copyright (c) 2018-2019 DaPorkchop_")
                    .info("")
                    .info("Usage:")
                    .info("  java [vm arguments]... -jar regionmerger.jar <mode> [mode arguments]...")
                    .info("")
                    .info("Modes:")
                    .info("  help:")
                    .info("    Shows this help message.");

            MODES.forEach((key, mode) -> mode.printUsage(log));
            return;
        }

        Mode mode = MODES.get(args[0]);
        if (mode == null) {
            logger.error("Unknown mode: \"%s\"", args[0])
                    .error("Use \"--help\" for a list of modes and what they do.");
            return;
        }
        Arguments arguments = mode.arguments();
        arguments.load(Arrays.asList(Arrays.copyOfRange(args, 1, args.length)).iterator());
        mode.run(arguments);

        logger.success("Done!");
    }
}

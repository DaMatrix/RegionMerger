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

package net.daporkchop.regionmerger;

import net.daporkchop.lib.logging.LogAmount;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.logging.Logging;
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

/**
 * @author DaPorkchop_
 */
public class RegionMerger implements Logging {
    public static final Map<String, Mode> MODES = new HashMap<String, Mode>() {
        {
            this.put("add", new Add());
            this.put("findmissing", new FindMissing());
            this.put("merge", new Merge());
            this.put("optimize", new Optimize());

            this.put("deletefromfile", new DeleteFromFile());
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
            log.info("RegionMerger v0.1.1")
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

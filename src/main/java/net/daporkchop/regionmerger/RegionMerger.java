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

import net.daporkchop.lib.logging.Logging;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.mode.findmissing.FindMissing;
import net.daporkchop.regionmerger.mode.merge.Merge;
import net.daporkchop.regionmerger.mode.optimize.Optimize;
import net.daporkchop.regionmerger.option.Arguments;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author DaPorkchop_
 */
public class RegionMerger implements Logging {
    public static final Map<String, Mode> MODES = new HashMap<String, Mode>()   {
        {
            this.put("findmissing", new FindMissing());
            this.put("merge", new Merge());
            this.put("optimize", new Optimize());
        }
    };

    public static void main(String... args) throws IOException {
        logger.enableANSI();

        Thread.setDefaultUncaughtExceptionHandler((thread, e) -> {
            logger.channel(thread.getName()).alert(e);
            System.exit(1);
        });

        if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
            /*logger.channel("Help")
                  .info("PorkRegionMerger v0.0.8")
                  .info("")
                  .info("--help                  Show this help message")
                  .info("--input=<path>          Add an input directory, must be a path to the root of a Minecraft save")
                  .info("--output=<path>         Set the output directory")
                  .info("--mode=<mode>           Set the mode (defaults to merge)")
                  .info("--areaCenterX=<x>       Set the center of the area along the X axis (only for area mode) (in chunks)")
                  .info("--areaCenterZ=<z>       Set the center of the area along the Z axis (only for area mode) (in chunks)")
                  .info("--areaRadius=<r>        Set the radius of the area (only for area mode) (in chunks)")
                  .info("--findMinX=<minX>       Set the min X coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMinZ=<minZ>       Set the min Z coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMaxX=<minX>       Set the max X coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMaxZ=<minZ>       Set the max Z coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--suppressAreaWarnings  Hide warnings for chunks with no inputs (only for area mode)")
                  .info("--skipincomplete        Skip incomplete regions (only for merge mode)")
                  .info("--forceoverwrite        Forces the merger to overwrite all currently present chunks (only for add mode)")
                  .info("--inputorder            Prioritize inputs by their input order rather than by file age (only for add mode)")
                  .info("--skipcopy              Slower, can fix some issues (only for merge mode)")
                  .info("--verbose   (-v)        Print more messages to your console (if you like spam ok)")
                  .info("-j=<threads>            The number of worker threads (only for add mode, defaults to cpu count)")
                  .info("")
                  .info("  Modes:  merge         Simply merges all chunks from all regions into the output")
                  .info("          area          Merges all chunks in a specified area into the output")
                  .info("          findmissing   Finds all chunks that aren't defined in an input and dumps them to a json file. Uses settings from area mode.")
                  .info("          add           Add all the chunks from every input into the output world, without removing any");*/
            return;
        }

        Mode mode = MODES.get(args[0]);
        if (mode == null)   {
            logger.error("Unknown mode: \"%s\"", args[0]);
            return;
        }
        Arguments arguments = mode.arguments();
        arguments.load(Arrays.asList(Arrays.copyOfRange(args, 1, args.length)).iterator());
        mode.run(arguments);

        logger.success("Done!");
    }
}

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

import lombok.NonNull;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.IOException;

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

        System.out.println(args.get(Option.DESTINATION).regions());
    }
}

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

package net.daporkchop.regionmerger.mode.findmissing;

import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.OverclockedRegionFile;
import net.daporkchop.regionmerger.mode.Mode;
import net.daporkchop.regionmerger.option.Arguments;
import net.daporkchop.regionmerger.option.Option;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static java.lang.Math.min;

/**
 * @author DaPorkchop_
 */
public class FindMissing implements Mode {
    protected static final Option.Int MIN_X = Option.integer("-minX", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int MIN_Z = Option.integer("-minZ", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int MAX_X = Option.integer("-maxX", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    protected static final Option.Int MAX_Z = Option.integer("-maxZ", Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);

    @Override
    public void printUsage(@NonNull Logger logger) {
        logger.info("findmissing:")
                .info("  Searches for missing chunks within a given area and saves them to a file (missingchunks.json).")
                .info("")
                .info("  Usage: findmissing [options] <path>")
                .info("")
                .info("  Options:")
                .info("  --minX <minX>       Set the min X coord to check (in regions) (inclusive)")
                .info("  --minZ <minZ>       Set the min Z coord to check (in regions) (inclusive)")
                .info("  --maxX <minX>       Set the max X coord to check (in regions) (inclusive)")
                .info("  --maxZ <minZ>       Set the max Z coord to check (in regions) (inclusive)");
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, false, MIN_X, MIN_Z, MAX_X, MAX_Z);
    }

    @Override
    public String name() {
        return "findmissing";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final int minX = args.get(MIN_X);
        final int minZ = args.get(MIN_Z);
        final int maxX = args.get(MAX_X);
        final int maxZ = args.get(MAX_Z);
        final World world = args.getDestination();
        if (min(minX, min(minZ, min(maxX, maxZ))) == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("minX, minZ, maxX and maxZ must all be set!");
        } else if (maxX < minX) {
            throw new IllegalArgumentException("maxX must be greater than or equal to minX!");
        } else if (maxZ < minZ) {
            throw new IllegalArgumentException("maxZ must be greater than or equal to minZ!");
        }

        final int deltaX = maxX - minX;
        final int deltaZ = maxZ - minZ;

        if (true) {
            throw new UnsupportedOperationException("findmissing mode is currently unimplemented.");
        }

        /*Arrays.stream(PArrays.filled(deltaX, (int x) -> x + minX))
              .boxed()
              .flatMap(x -> Arrays.stream(PArrays.filled(deltaZ, Vec2i[]::new, (int z) -> new Vec2i(x, z + minZ))))
              .map(v -> new Tuple<>(v, world.getAsFile(v)))
              .filter(t -> t.getB().exists())

        byte[] json = world.regions().parallelStream()
                .flatMap((IOFunction<Vec2i, Stream<Vec2i>>) regionPos -> {
                    try (OverclockedRegionFile region = new OverclockedRegionFile(world.getAsFile(regionPos), true, false)) {

                    }
                })
                .map(v -> String.format("{\"x\":%d,\"z\":%d}", v.getX(), v.getY()))
                .collect(() -> new StringJoiner(",", "[", "]"), StringJoiner::add, StringJoiner::merge)
                .toString().getBytes(StandardCharsets.UTF_8);*/
    }
}

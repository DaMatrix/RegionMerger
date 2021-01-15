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

package net.daporkchop.regionmerger;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;

import static net.daporkchop.mcworldlib.format.anvil.region.RegionConstants.*;

/**
 * Different methods of prioritizing chunks.
 *
 * @author DaPorkchop_
 */
public enum Sort {
    YOUNGEST {
        @Override
        public ByteBuf select(@NonNull ByteBuf[] regions, int count, int x, int z) {
            final int offsetIndex = getOffsetIndex(x, z);
            final int timestampIndex = getTimestampIndex(x, z);

            ByteBuf best = null;
            int bestValue = Integer.MIN_VALUE;
            for (int i = 0; i < count; i++) {
                ByteBuf region = regions[i];
                if (region.getInt(offsetIndex) != 0) { //chunk exists
                    int value = region.getInt(timestampIndex);
                    if (value > bestValue) {
                        bestValue = value;
                        best = region;
                    }
                }
            }
            return best;
        }
    },
    OLDEST {
        @Override
        public ByteBuf select(@NonNull ByteBuf[] regions, int count, int x, int z) {
            final int offsetIndex = getOffsetIndex(x, z);
            final int timestampIndex = getTimestampIndex(x, z);

            ByteBuf best = null;
            int bestValue = Integer.MAX_VALUE;
            for (int i = 0; i < count; i++) {
                ByteBuf region = regions[i];
                if (region.getInt(offsetIndex) != 0) { //chunk exists
                    int value = region.getInt(timestampIndex);
                    if (value < bestValue) {
                        bestValue = value;
                        best = region;
                    }
                }
            }
            return best;
        }
    },
    INPUT_ORDER {
        @Override
        public ByteBuf select(@NonNull ByteBuf[] regions, int count, int x, int z) {
            final int offsetIndex = getOffsetIndex(x, z);

            for (int i = 0; i < count; i++) {
                ByteBuf region = regions[i];
                if (region.getInt(offsetIndex) != 0) { //region exists
                    return region;
                }
            }
            return null;
        }
    };

    public abstract ByteBuf select(@NonNull ByteBuf[] regions, int count, int x, int z);
}

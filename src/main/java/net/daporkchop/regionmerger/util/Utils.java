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

package net.daporkchop.regionmerger.util;

import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.misc.string.PStrings;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Utils {
    public String formatDuration(long duration) {
        duration = notNegative(duration, "duration");

        Object[] args = {
                duration / (1000L * 60L * 60L * 24L),
                (duration / (1000L * 60L * 60L)) % 24L,
                (duration / (1000L * 60L)) % 60L,
                (duration / 1000L) % 60L,
                duration % 1000L
        };

        if (duration >= 1000L * 60L * 60L * 24L) {
            return PStrings.fastFormat("%1$dd %2$02dh %3$02dm %4$02ds %5$03dms", args);
        } else if (duration >= 1000L * 60L * 60L) {
            return PStrings.fastFormat("%2$02dh %3$02dm %4$02ds %5$03dms", args);
        } else if (duration >= 1000L * 60L) {
            return PStrings.fastFormat("%3$02dm %4$02ds %5$03dms", args);
        } else if (duration >= 1000L) {
            return PStrings.fastFormat("%4$02ds %5$03dms", args);
        } else {
            return PStrings.fastFormat("%5$03dms", args);
        }
    }
}

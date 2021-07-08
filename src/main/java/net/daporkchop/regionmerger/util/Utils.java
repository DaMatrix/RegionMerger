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

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import net.daporkchop.lib.common.misc.string.PStrings;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
@UtilityClass
public class Utils {
    protected static final OpenOption[] WRITE_OPEN_OPTIONS = { StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };
    protected static final CopyOption[] REPLACE_COPY_OPTIONS = { StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE };

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

    public void writeFully(@NonNull FileChannel channel, @NonNull ByteBuf data) throws IOException {
        do {
            data.readBytes(channel, data.readableBytes());
        } while (data.isReadable());
    }

    public void writeAndReplace(@NonNull Path dstPath, @NonNull ByteBuf data) throws IOException {
        Path tmpPath = dstPath.resolveSibling(dstPath.getFileName() + ".tmp");

        //write to temporary file
        try (FileChannel channel = FileChannel.open(tmpPath, WRITE_OPEN_OPTIONS)) {
            writeFully(channel, data);
        }

        //replace real file (atomically)
        Files.move(tmpPath, dstPath, REPLACE_COPY_OPTIONS);
    }
}

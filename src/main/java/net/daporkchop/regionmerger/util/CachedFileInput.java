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
 */

package net.daporkchop.regionmerger.util;

import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataIn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

@Getter
public class CachedFileInput extends DataIn {
    private ByteBuffer buffer;

    public CachedFileInput(@NonNull File file) throws IOException {
        if (file.length() >= Integer.MAX_VALUE) {
            throw new IllegalStateException(String.format("File %s too big to be loaded!", file.getAbsolutePath()));
        }
        this.buffer = ByteBuffer.allocateDirect((int) file.length());
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ))  {
            channel.read(this.buffer, 0L);
            this.buffer.flip();
        } catch (IOException e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        this.buffer = null;
    }

    @Override
    public int read() throws IOException {
        if (this.buffer == null) {
            return -1;
        } else if (!this.buffer.hasRemaining()) {
            this.close();
            return -1;
        } else {
            return this.buffer.get() & 0xFF;
        }
    }
}

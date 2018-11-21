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

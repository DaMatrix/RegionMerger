package net.daporkchop.regionmerger.anvil.pork;

import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.regionmerger.RegionMerger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author DaPorkchop_
 */
public class PorkAnvilArchive {
    private final RandomAccessFile file;
    private final DataOut out;
    private final AtomicLong chunkCount = new AtomicLong(0L);
    private final Lock writeLock = new ReentrantLock();
    private volatile boolean running = true;

    public PorkAnvilArchive(@NonNull File file, /*@NonNull CompressionMode mode, */int compressionId, int bufferSize) throws IOException {
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Not a file: %s", file.getAbsolutePath()));
            }
        } else {
            File parent = file.getParentFile();
            if (!parent.exists() && !parent.mkdirs()) {
                throw new IllegalStateException(String.format("Unable to create directory: %s", parent.getAbsolutePath()));
            } else if (!file.createNewFile()) {
                throw new IllegalStateException(String.format("Unable to create file: %s", file.getAbsolutePath()));
            }
        }
        this.file = new RandomAccessFile(file, "rw");
        this.file.write(compressionId & 0xFF);
        OutputStream out = new BufferedChannelOutput(bufferSize, this.file.getChannel(), 9L);
        //out = mode.outputCreator.apply(bufferSize, out);
        out = PorkRegion.COMPRESSION_IDS.getOrDefault((byte) compressionId, Compression.NONE).deflate(out);
        this.out = DataOut.wrap(out);
    }

    public void writeRegion(int x, int z, @NonNull InputStream[] inputs) throws IOException {
        if (!this.running) {
            throw new IllegalStateException("already closed");
        }
        this.writeLock.lock();
        try {
            for (int xx = 31; xx >= 0; xx--) {
                for (int zz = 31; zz >= 0; zz--) {
                    InputStream in = inputs[(xx << 5) | zz];
                    if (in == null) {
                        continue;
                    }
                    this.chunkCount.incrementAndGet();
                    this.out.writeInt((x << 5) | xx);
                    this.out.writeInt((z << 5) | zz);
                    byte[] b = RegionMerger.BUFFER_CACHE.get();
                    int i;
                    int j = 0;
                    while ((i = in.read(b, j, b.length - j)) != -1) {
                        j += i;
                    }
                    this.out.write(b, 0, j);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public void close() throws IOException {
        if (!this.running) {
            throw new IllegalStateException("already closed");
        }
        /*this.running = false;
        this.out.close();
        this.file.close();*/
        this.writeLock.lock();
        try {
            this.running = false;
            this.out.flush();
            this.file.seek(1L);
            this.file.writeLong(this.chunkCount.get());
            this.out.close();
            this.file.close();
        } finally {
            this.writeLock.unlock();
        }
    }

    /*@RequiredArgsConstructor
    public enum CompressionMode {
        FULL_STREAM(null),
        PER_REGION((bufferSize, function) -> (regionPos, inputs) -> {
            ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
            try (OutputStream out = function.apply(new ByteBufferOutputStream(buffer));
                 DataOut dataOut = new DataOut(out)) {
                dataOut.write(regionPos.x);
                dataOut.write(regionPos.z);
                for (int x = 31; x >= 0; x--)   {
                    for (int z = 31; z >= 0; z--)   {
                        InputStream in = inputs[(x << 5) | z];
                        if (in == null) {
                            continue;
                        }
                        dataOut.writeShort((short) ((x << 5) | z));
                    }
                }
                dataOut.writeShort((short) 1024); //end of stream
            }
        });

        @NonNull
        private final IntegerObjectObjectBiFunction<IOEFunction<OutputStream, OutputStream>, ThrowingBiConsumer<Pos, InputStream[], IOException>> outputCreator;
    }*/

    public static class BufferedChannelOutput extends OutputStream {
        private final ByteBuffer buffer;
        private final int size;
        private final FileChannel channel;
        private AtomicInteger off = new AtomicInteger();
        private AtomicLong fileOff;

        public BufferedChannelOutput(int bufferSize, @NonNull FileChannel channel) {
            this(bufferSize, channel, 0L);
        }

        public BufferedChannelOutput(int bufferSize, @NonNull FileChannel channel, long offset) {
            this.buffer = ByteBuffer.allocateDirect(bufferSize);
            this.size = bufferSize;
            this.channel = channel;
            this.fileOff = new AtomicLong(offset);
        }

        @Override
        public void write(int b) throws IOException {
            this.buffer.put(this.off.getAndIncrement(), (byte) b);
            if (this.off.get() == this.size) {
                this.flush();
            }
        }

        @Override
        public void flush() throws IOException {
            this.buffer.limit(this.off.getAndSet(0));
            //this.buffer.flip();
            this.channel.write(this.buffer, this.fileOff.getAndAdd(this.buffer.limit()));
            this.buffer.clear();
        }

        @Override
        public void close() throws IOException {
            this.flush();
            this.channel.close();
        }
    }
}

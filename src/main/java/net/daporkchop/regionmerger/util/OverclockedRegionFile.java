package net.daporkchop.regionmerger.util;

import com.zaxxer.sparsebits.SparseBitSet;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.encoding.compression.CompressionHelper;
import net.daporkchop.lib.primitive.map.ByteObjectMap;
import net.daporkchop.lib.primitive.map.hashmap.ByteObjectHashMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.*;

/**
 * @author DaPorkchop_
 */
public class OverclockedRegionFile {
    public static final ByteObjectMap<CompressionHelper> COMPRESSION_IDS = new ByteObjectHashMap<>();
    private static final int CHUNK_HEADER_SIZE = 5;
    private static final int SECTOR_BYTES = 4096;
    private static final int SECTOR_INTS = SECTOR_BYTES >> 2;
    private static final byte EMPTY_SECTOR[] = new byte[SECTOR_BYTES];
    private static final Integer[] INTEGER_LOOKUP = new Integer[32 * 32];

    static {
        //Compatibility with legacy RegionFile
        COMPRESSION_IDS.put((byte) 1, CompressionHelper.builder("GZip", "RegionFile legacy compat")
                .setInputStreamWrapperSimple(GZIPInputStream::new)
                .setOutputStreamWrapperSimple(GZIPOutputStream::new)
                .build());
        COMPRESSION_IDS.put((byte) 2, CompressionHelper.builder("ZLIB", "RegionFile legacy compat")
                .setInputStreamWrapperSimple(InflaterInputStream::new)
                .setOutputStreamWrapperSimple(DeflaterOutputStream::new)
                .build());

        //new things
        COMPRESSION_IDS.put((byte) 0, Compression.NONE);
        COMPRESSION_IDS.put((byte) 3, Compression.GZIP_LOW);
        COMPRESSION_IDS.put((byte) 4, Compression.GZIP_NORMAL);
        COMPRESSION_IDS.put((byte) 5, Compression.GZIP_HIGH);
        COMPRESSION_IDS.put((byte) 6, Compression.BZIP2_LOW);
        COMPRESSION_IDS.put((byte) 7, Compression.BZIP2_NORMAL);
        COMPRESSION_IDS.put((byte) 8, Compression.BZIP2_HIGH);
        COMPRESSION_IDS.put((byte) 9, Compression.DEFLATE_LOW);
        COMPRESSION_IDS.put((byte) 10, Compression.DEFLATE_NORMAL);
        COMPRESSION_IDS.put((byte) 11, Compression.DEFLATE_HIGH);
        COMPRESSION_IDS.put((byte) 12, Compression.LZ4_BLOCK);
        COMPRESSION_IDS.put((byte) 13, Compression.LZMA_LOW);
        COMPRESSION_IDS.put((byte) 14, Compression.LZMA_NORMAL);
        COMPRESSION_IDS.put((byte) 15, Compression.LZMA_HIGH);

        for (int i = INTEGER_LOOKUP.length - 1; i >= 0; i--) {
            INTEGER_LOOKUP[i] = i;
        }
    }

    private final RandomAccessFile file;
    private final FileChannel channel;
    private final MappedByteBuffer index;

    private final SparseBitSet occupiedSectors = new SparseBitSet();
    @Getter
    private final long lastModified;
    private final Map<Integer, ReadWriteLock> locks = Collections.synchronizedMap(new WeakHashMap<>());

    public OverclockedRegionFile(@NonNull File file) throws IOException {
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Not a file: %s", file.getAbsolutePath()));
            }
        } else if (!file.getParentFile().mkdirs() || !file.createNewFile()) {
            throw new IllegalStateException(String.format("Unable to create file: %s", file.getAbsolutePath()));
        }
        this.lastModified = file.lastModified();
        this.file = new RandomAccessFile(file, "rw");
        if (this.file.length() < SECTOR_BYTES) {
            //initialize
            this.file.seek(0L);
            //write the chunk offset table
            this.file.write(EMPTY_SECTOR);
            //write another sector for the timestamp info
            this.file.write(EMPTY_SECTOR);
        }
        if ((this.file.length() & 0xfff) != 0) {
            //the file size is not a multiple of 4KB, grow it
            for (int i = 0; i < (file.length() & 0xfff); ++i) {
                this.file.write((byte) 0);
            }
        }
        this.channel = this.file.getChannel();
        this.index = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, SECTOR_BYTES);

        if (true) {
            this.file.seek(0L);
            System.out.printf("Vanilla: %d\n", this.file.readInt());
            System.out.printf("mmapped: %d\n", this.index.getInt(0));

            this.file.seek((31 + 31 * 32) << 2);
            System.out.printf("Vanilla: %d\n", this.file.readInt());
            System.out.printf("mmapped: %d\n", this.index.getInt((31 + 31 * 32) << 2));
        }

        this.occupiedSectors.set(0); //chunk offset table
        this.occupiedSectors.set(1); //last modified info
        //init occupied sectors bitset
        for (int i = SECTOR_INTS - 1; i >= 0; i--) {
            int offset = this.index.getInt(i << 2);
            if (offset != 0) {
                this.occupiedSectors.set(offset >> 8, (offset >> 8) + (offset & 0xFF) + 1);
            }
        }
    }

    private static void ensureInBounds(int x, int z) {
        if (x < 0 || x >= 32 || z < 0 || z >= 32) {
            throw new IllegalArgumentException(String.format("Coordinates out of bounds: (%d,%d)", x, z));
        }
    }

    public InputStream readData(int x, int z) throws IOException {
        ensureInBounds(x, z);
        int offset = this.getOffset(x, z);
        if (offset == 0) {
            return null;
        }

        Lock lock = this.getLock(x, z).readLock();
        lock.lock();
        try {
            int sectorNumber = offset >> 8;
            int numSectors = offset & 0xFF;

            ByteBuffer buffer = ByteBuffer.allocateDirect(numSectors * SECTOR_BYTES);
            this.channel.read(buffer, sectorNumber * SECTOR_BYTES);
            buffer.flip();
            int length = buffer.getInt();
            buffer.limit(length);
            byte version = buffer.get();
            /*this.file.seek(sectorNumber * SECTOR_BYTES);
            int length = this.file.readInt();
            byte version = this.file.readByte();*/
            CompressionHelper compression = COMPRESSION_IDS.get(version);
            if (compression == null) {
                throw new IOException(String.format("Found invalid chunk version: %d", version % 0xFF));
            } else {
                System.out.printf("[DEBUG] Chunk (%d,%d) is using compression: %s\n", x, z, compression);
            }
            /*byte[] b = new byte[length - 1];
            this.file.readFully(b);*/
            return compression.inflate(new ByteBufferInputStream(buffer));
            //return compression.inflate(new ByteArrayInputStream(b));
        } finally {
            lock.unlock();
        }
    }

    public boolean hasChunk(int x, int z) {
        return this.getOffset(x, z) != 0;
    }

    private int getOffset(int x, int z) {
        return this.index.getInt((x + z * 32) * 4);
    }

    private ReadWriteLock getLock(int x, int z) {
        return this.locks.computeIfAbsent(INTEGER_LOOKUP[x + z * 32], i -> new ReentrantReadWriteLock());
    }

    private void setOffset(int x, int z, int offset) {
        this.index.putInt((x + z * 32) * 4, offset);
    }

    private void setTimestamp(int x, int z, int value) {
        this.index.putInt(SECTOR_BYTES + (x + z * 32) * 4, value);
    }

    public void close() throws IOException {
        this.index.force();
        this.channel.close();
        this.file.close();
    }

    private static final class ByteBufferInputStream extends InputStream {
        private final ByteBuffer bb;

        private ByteBufferInputStream(ByteBuffer var1) {
            this.bb = var1;
        }

        public int read() throws IOException {
            if (this.bb == null) {
                throw new IOException("read on a closed InputStream");
            } else {
                return this.bb.remaining() == 0 ? -1 : this.bb.get() & 255;
            }
        }

        public int read(byte[] var1) throws IOException {
            if (this.bb == null) {
                throw new IOException("read on a closed InputStream");
            } else {
                return this.read(var1, 0, var1.length);
            }
        }

        public int read(byte[] var1, int var2, int var3) throws IOException {
            if (this.bb == null) {
                throw new IOException("read on a closed InputStream");
            } else if (var1 == null) {
                throw new NullPointerException();
            } else if (var2 >= 0 && var3 >= 0 && var3 <= var1.length - var2) {
                if (var3 == 0) {
                    return 0;
                } else {
                    int var4 = Math.min(this.bb.remaining(), var3);
                    if (var4 == 0) {
                        return -1;
                    } else {
                        this.bb.get(var1, var2, var4);
                        return var4;
                    }
                }
            } else {
                throw new IndexOutOfBoundsException();
            }
        }

        public long skip(long var1) throws IOException {
            if (this.bb == null) {
                throw new IOException("skip on a closed InputStream");
            } else if (var1 <= 0L) {
                return 0L;
            } else {
                int var3 = (int) var1;
                int var4 = Math.min(this.bb.remaining(), var3);
                this.bb.position(this.bb.position() + var4);
                return (long) var3;
            }
        }

        public int available() throws IOException {
            if (this.bb == null) {
                throw new IOException("available on a closed InputStream");
            } else {
                return this.bb.remaining();
            }
        }

        public synchronized void mark(int var1) {
        }

        public synchronized void reset() throws IOException {
            throw new IOException("mark/reset not supported");
        }

        public boolean markSupported() {
            return false;
        }
    }
}

package net.daporkchop.regionmerger.anvil.mojang;

import com.zaxxer.sparsebits.SparseBitSet;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.ByteBufferInputStream;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.encoding.compression.CompressionHelper;
import net.daporkchop.lib.primitive.map.ByteObjectMap;
import net.daporkchop.lib.primitive.map.hashmap.ByteObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

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

        if (false) {
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

        ReadWriteLock lock = this.getLock(x, z);
        lock.readLock().lock();
        try {
            int sectorNumber = offset >> 8;
            int numSectors = offset & 0xFF;

            ByteBuffer buffer = ByteBuffer.allocateDirect(numSectors * SECTOR_BYTES);
            this.channel.read(buffer, sectorNumber * SECTOR_BYTES);
            buffer.flip();
            int length = buffer.getInt();
            //TODO: is this needed lol
            // buffer.limit(length);
            byte version = buffer.get();
            CompressionHelper compression = COMPRESSION_IDS.get(version);
            if (compression == null) {
                throw new IOException(String.format("Found invalid chunk version: %d", version & 0xFF));
            }/* else {
                System.out.printf("[DEBUG] Chunk (%d,%d) is using compression: %s\n", x, z, compression);
            }*/
            //System.out.printf("Data length: %d, sectors length: %d\n", length, sectorNumber * SECTOR_BYTES);
            return compression.inflate(new ByteBufferInputStream(buffer));
        } finally {
            lock.readLock().unlock();
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
}

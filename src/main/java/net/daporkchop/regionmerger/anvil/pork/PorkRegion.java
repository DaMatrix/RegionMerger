package net.daporkchop.regionmerger.anvil.pork;

import com.zaxxer.sparsebits.SparseBitSet;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.ByteBufferInputStream;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.encoding.compression.CompressionHelper;
import net.daporkchop.lib.primitive.map.ByteObjectMap;
import net.daporkchop.lib.primitive.map.hashmap.ByteObjectHashMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Used for the normal mode of PorkAnvil.
 *
 * @author DaPorkchop_
 */
public class PorkRegion {
    public static final ByteObjectMap<CompressionHelper> COMPRESSION_IDS = new ByteObjectHashMap<>();

    /**
     * The size of a single sector
     */
    private static final int SECTOR_BYTES = 1024;
    /**
     * The data for a blank sector
     */
    private static final byte[] EMPTY_SECTOR = new byte[SECTOR_BYTES];
    /**
     * The number of bytes per header entry
     * <p>
     * 3 bytes => offset
     * 2 bytes => sector count
     * 1 byte  => compression id
     */
    private static final int HEADER_BYTES = 6;
    private static final int SIZE = 32;
    private static final int FULL_HEADER_SIZE = HEADER_BYTES * (SIZE * SIZE);
    private static final int HEADER_SECTOR_COUNT = FULL_HEADER_SIZE / SECTOR_BYTES;
    private static final Integer[] INTEGER_LOOKUP = new Integer[SIZE * SIZE];

    static {
        COMPRESSION_IDS.put((byte) 0, Compression.NONE);
        //COMPRESSION_IDS.put((byte) 1, Compression.GZIP_LOW);
        COMPRESSION_IDS.put((byte) 2, Compression.GZIP_NORMAL);
        COMPRESSION_IDS.put((byte) 3, Compression.GZIP_HIGH);
        COMPRESSION_IDS.put((byte) 4, Compression.BZIP2_LOW);
        COMPRESSION_IDS.put((byte) 5, Compression.BZIP2_NORMAL);
        COMPRESSION_IDS.put((byte) 6, Compression.BZIP2_HIGH);
        //COMPRESSION_IDS.put((byte) 7, Compression.DEFLATE_LOW);
        COMPRESSION_IDS.put((byte) 8, Compression.DEFLATE_NORMAL);
        COMPRESSION_IDS.put((byte) 9, Compression.DEFLATE_HIGH);
        COMPRESSION_IDS.put((byte) 10, Compression.LZ4_BLOCK);
        COMPRESSION_IDS.put((byte) 10, Compression.LZ4_FRAMED_64KB);
        COMPRESSION_IDS.put((byte) 10, Compression.LZ4_FRAMED_256KB);
        COMPRESSION_IDS.put((byte) 10, Compression.LZ4_FRAMED_1MB);
        COMPRESSION_IDS.put((byte) 10, Compression.LZ4_FRAMED_4MB);
        COMPRESSION_IDS.put((byte) 11, Compression.LZMA_LOW);
        COMPRESSION_IDS.put((byte) 12, Compression.LZMA_NORMAL);
        COMPRESSION_IDS.put((byte) 13, Compression.LZMA_HIGH);

        for (int i = INTEGER_LOOKUP.length - 1; i >= 0; i--) {
            INTEGER_LOOKUP[i] = i;
        }
    }

    private final SparseBitSet occupiedSectors = new SparseBitSet();
    private final ReadWriteLock sectorsLock = new ReentrantReadWriteLock();
    private final Map<Integer, ReadWriteLock> locks = Collections.synchronizedMap(new WeakHashMap<>());

    private final RandomAccessFile file;
    private final File theFile;
    private final FileChannel channel;
    private final MappedByteBuffer index;

    public PorkRegion(@NonNull File file) throws IOException {
        this.theFile = file;
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Not a file: %s", file.getAbsolutePath()));
            }
        } else {
            File parent = file.getParentFile();
            if (!parent.exists() && !parent.mkdirs())   {
                throw new IllegalStateException(String.format("Unable to create directory: %s", parent.getAbsolutePath()));
            } else if (!file.createNewFile())   {
                throw new IllegalStateException(String.format("Unable to create file: %s", file.getAbsolutePath()));
            }
        }
        this.file = new RandomAccessFile(file, "rw");
        {
            if (this.file.length() == 0L) {
                //optimize a bit for first write
                for (int i = 0; i < 6; i++) {
                    this.file.write(EMPTY_SECTOR);
                }
            }
            //more of a sanity check here
            long length;
            while ((length = this.file.length()) < FULL_HEADER_SIZE/* || (length & (SECTOR_BYTES - 1)) != 0*/) {
                this.file.write(0);
            }
        }
        this.channel = this.file.getChannel();
        this.index = this.channel.map(FileChannel.MapMode.READ_WRITE, 0L, FULL_HEADER_SIZE);

        //mark header sectors
        for (int i = 0; i < 6; i++) {
            this.occupiedSectors.set(i);
        }
    }

    private static void ensureInBounds(int x, int z) {
        if (x < 0 || x >= 32 || z < 0 || z >= 32) {
            throw new IllegalArgumentException(String.format("Coordinates out of bounds: (%d,%d)", x, z));
        }
    }

    public InputStream read(int x, int z) throws IOException {
        ensureInBounds(x, z);
        long offset = this.getOffset(x, z);
        if (offset == 0L) {
            return null;
        }

        ReadWriteLock lock = this.getLock(x, z);
        lock.readLock().lock();
        try {
            int sectorOffset = (int) (offset >>> 24);
            int numSectors = ((int) offset >>> 8) & 0xFFFF;
            int version = (int) (offset & 0xFFL);

            ByteBuffer buffer = ByteBuffer.allocateDirect(numSectors * SECTOR_BYTES);
            this.channel.read(buffer, sectorOffset * SECTOR_BYTES);
            buffer.flip();

            CompressionHelper compression = COMPRESSION_IDS.get((byte) (version & 0x7F));
            if (compression == null) {
                throw new IOException(String.format("Found invalid chunk version: %d", version & 0xFF));
            }/* else {
                System.out.printf("[DEBUG] Chunk (%d,%d) is using compression: %s\n", x, z, compression);
            }*/
            return compression.inflate(new ByteBufferInputStream(buffer));
        } finally {
            lock.readLock().unlock();
        }
    }

    public OutputStream write(int x, int z, @NonNull CompressionHelper compression) throws IOException {
        AtomicInteger i = new AtomicInteger(-1);
        COMPRESSION_IDS.forEach((id, helper) -> {
            if (helper == compression)  {
                i.set(id & 0xFF);
            }
        });
        if (i.get() == -1)  {
            throw new IllegalArgumentException(String.format("Unregistered compression format: %s", compression));
        } else {
            return this.write(x, z, i.get());
        }
    }

    public OutputStream write(int x, int z, int compressionId) throws IOException {
        ensureInBounds(x, z);

        CompressionHelper compression = COMPRESSION_IDS.get((byte) compressionId);
        if (compression == null)    {
            throw new IllegalArgumentException(String.format("Invalid compression id: %d", compressionId));
        }

        ReadWriteLock lock = this.getLock(x, z);
        lock.writeLock().lock();
        return compression.deflate(new RegionOutput(lock, x, z, compressionId));
    }

    private void doWrite(@NonNull RegionOutput output) throws IOException {
        try {
            long offset = this.getOffset(output.x, output.z);
            int sectorOffsetOld = (int) (offset >>> 24);
            int numSectorsOld = ((int) offset >>> 8) & 0xFFFF;
            int versionOld = (int) (offset & 0xFFL);
            int numSectors = output.buffers.size();
            if ((numSectors & 0xFFFF) != numSectors) {
                throw new IllegalStateException(String.format("Data too big! Would need %d sectors...", numSectors));
            }
            if (numSectors <= numSectorsOld) {
                //we can write now
                this.theActualRealDoWrite(sectorOffsetOld * SECTOR_BYTES, output.buffers);
                if (versionOld != output.version) {
                    this.setOffset(output.x, output.z, ((offset >>> 8L) << 8L) | (output.version & 0xFF));
                }
                if (numSectors < numSectorsOld) {
                    //deallocate old sectors
                    this.sectorsLock.writeLock().lock();
                    try {
                        for (int i = numSectors; i < numSectorsOld; i++) {
                            this.occupiedSectors.clear(sectorOffsetOld + i);
                        }
                    } finally {
                        this.sectorsLock.writeLock().unlock();
                    }
                }
            } else if (numSectors > numSectorsOld) {
                int sectorOffsetNew = -1;
                this.sectorsLock.writeLock().lock();
                try {
                    //deallocate old sectors
                    for (int i = 0; i < numSectorsOld; i++) {
                        this.occupiedSectors.clear(sectorOffsetOld + i);
                    }
                    //allocate new ones
                    int i = 0;
                    SEARCH:
                    do {
                        i = this.occupiedSectors.nextClearBit(i);
                        for (int j = 1; j < numSectors; j++) {
                            //don't check first sector, that's why j starts at 1
                            if (this.occupiedSectors.get(i + j)) {
                                i += j;
                                continue SEARCH;
                            }
                        }
                        sectorOffsetNew = i;
                        for (int j = 0; j < numSectors; j++) {
                            this.occupiedSectors.set(i + j);
                        }
                    } while (sectorOffsetNew == -1);
                } finally {
                    this.sectorsLock.writeLock().unlock();
                }
                //write data and stuff
                this.theActualRealDoWrite(sectorOffsetNew * SECTOR_BYTES, output.buffers);
                this.setOffset(output.x, output.z, ((long) sectorOffsetNew << 24L) | ((numSectors & 0xFFFF) << 8) | (output.version & 0xFFL));
            }
        } finally {
            output.lock.writeLock().unlock();
        }
    }

    private void theActualRealDoWrite(long pos, @NonNull List<ByteBuffer> buffers) throws IOException {
        for (ByteBuffer buffer : buffers) {
            this.channel.write(buffer, pos);
            pos += SECTOR_BYTES;
        }
    }

    public void close() throws IOException {
        this.index.force();
        this.channel.close();
        this.file.close();
    }

    public boolean hasChunk(int x, int z) {
        return this.getOffset(x, z) != 0;
    }

    private long getOffset(int x, int z) {
        int i = ((x << 5) | z) * HEADER_BYTES;
        return ((this.index.getInt(i) & 0xFFFFFFFFL) << 16) | (this.index.getShort(i + 4) & 0xFFFF);
    }

    private ReadWriteLock getLock(int x, int z) {
        return this.locks.computeIfAbsent(INTEGER_LOOKUP[(x << 5) | z], i -> new ReentrantReadWriteLock());
    }

    private void setOffset(int x, int z, long offset) {
        int i = ((x << 5) | z) * HEADER_BYTES;
        this.index.putInt(i, (int) ((offset >>> 16L) & 0xFFFFFFFFL));
        this.index.putShort(i + 4, (short) (offset & 0xFFFFL));
    }

    public long getSize() throws IOException   {
        return this.file.length();
    }

    public File getFile()   {
        return this.theFile;
    }

    private final class RegionOutput extends OutputStream {
        private final ReadWriteLock lock;
        private final int x;
        private final int z;
        private final int version;
        private final List<ByteBuffer> buffers = new ArrayList<>();
        private ByteBuffer current;

        private RegionOutput(@NonNull ReadWriteLock lock, int x, int z, int version) {
            this.lock = lock;
            this.x = x;
            this.z = z;
            this.version = version;
        }

        @Override
        public void write(int b) throws IOException {
            if (this.current == null || this.current.position() == SECTOR_BYTES) {
                this.update(false);
            }
            this.current.put((byte) b);
        }

        @Override
        public void close() throws IOException {
            this.update(true);
            PorkRegion.this.doWrite(this);
        }

        private void update(boolean finished) {
            if (this.current != null) {
                this.current.flip();
                this.buffers.add(this.current);
            }
            if (!finished) {
                this.current = ByteBuffer.allocateDirect(SECTOR_BYTES);
            }
        }
    }
}

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

package net.daporkchop.regionmerger.anvil.mojang;

import com.zaxxer.sparsebits.SparseBitSet;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataIn;
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
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * A highly optimized rewrite of Mojang's original {@link RegionFile}, designed with backwards-compatibility in mind.
 * <p>
 * Of course, compression modes other than 1 and 2 (GZip and ZLIB respectively) won't work with a vanilla Minecraft implementation, so
 * they should be avoided.
 *
 * @author DaPorkchop_
 */
public class OverclockedRegionFile implements AutoCloseable {
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
        //COMPRESSION_IDS.put((byte) 2, Compression.DEFLATE_HIGH);

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
    private final ReadWriteLock sectorsLock = new ReentrantReadWriteLock();

    public OverclockedRegionFile(@NonNull File file) throws IOException {
        if (file.exists()) {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Not a file: %s", file.getAbsolutePath()));
            }
        } else if ((!file.getParentFile().exists() && !file.getParentFile().mkdirs()) || !file.createNewFile()) {
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
            buffer.limit(length + 5);
            byte version = buffer.get();
            CompressionHelper compression = COMPRESSION_IDS.get(version);
            if (compression == null) {
                throw new IOException(String.format("Found invalid chunk version: %d", version & 0xFF));
            } else if (false) {
                System.out.printf("[DEBUG] Chunk (%d,%d) is using compression: %s\n", x, z, compression);
            }
            //System.out.printf("Data length: %d, sectors length: %d\n", length, sectorNumber * SECTOR_BYTES);
            return compression.inflate(DataIn.wrap(buffer));
        } finally {
            lock.readLock().unlock();
        }
    }

    public OutputStream write(int x, int z, @NonNull CompressionHelper compression) throws IOException {
        AtomicInteger i = new AtomicInteger(-1);
        COMPRESSION_IDS.forEach((id, helper) -> {
            if (helper == compression) {
                i.set(id & 0xFF);
            }
        });
        if (i.get() == -1) {
            throw new IllegalArgumentException(String.format("Unregistered compression format: %s", compression));
        } else {
            return this.write(x, z, i.get());
        }
    }

    public OutputStream write(int x, int z, int compressionId) throws IOException {
        ensureInBounds(x, z);

        CompressionHelper compression = COMPRESSION_IDS.get((byte) compressionId);
        if (compression == null) {
            throw new IllegalArgumentException(String.format("Invalid compression id: %d", compressionId));
        }

        ReadWriteLock lock = this.getLock(x, z);
        lock.writeLock().lock();
        return compression.deflate(new RegionOutput(lock, x, z, compressionId));
    }

    private void doWrite(@NonNull RegionOutput output) throws IOException {
        try {
            int offset = this.getOffset(output.x, output.z);
            int sectorOffsetOld = offset >> 8;
            int numSectorsOld = offset & 0xFF;
            int numSectors = output.buffers.size();
            if ((numSectors & 0xFFFF) != numSectors) {
                throw new IllegalStateException(String.format("Data too big! Would need %d sectors...", numSectors));
            }
            if (numSectors <= numSectorsOld) {
                //we can write now
                this.theActualRealDoWrite(sectorOffsetOld * SECTOR_BYTES, output.buffers);
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
                this.setOffset(output.x, output.z, (sectorOffsetNew << 8) | (numSectors & 0xFF));
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

    @Override
    public void close() throws IOException {
        this.index.force();
        this.channel.close();
        this.file.close();
    }

    private final class RegionOutput extends OutputStream {
        private final ReadWriteLock lock;
        private final int x;
        private final int z;
        private final int version;
        private final List<ByteBuffer> buffers = new ArrayList<>();
        private ByteBuffer current;
        private int size;

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
            this.size++;
        }

        @Override
        public void close() throws IOException {
            this.update(true);
            this.buffers.get(0).putInt(0, this.size);
            OverclockedRegionFile.this.doWrite(this);
        }

        private void update(boolean finished) {
            if (this.current != null) {
                this.current.flip();
                this.buffers.add(this.current);
            }
            if (!finished) {
                boolean flag = this.current == null;
                this.current = ByteBuffer.allocateDirect(SECTOR_BYTES);
                if (flag) {
                    current.putInt(-1); //length
                    current.put((byte) this.version); //VERSION_DEFLATE
                }
            }
        }
    }
}

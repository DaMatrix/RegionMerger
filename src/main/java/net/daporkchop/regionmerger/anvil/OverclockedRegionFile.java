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
 *
 */

package net.daporkchop.regionmerger.anvil;

import com.zaxxer.sparsebits.SparseBitSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.lib.common.cache.SoftThreadCache;
import net.daporkchop.lib.common.cache.ThreadCache;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.common.util.PorkUtil;
import net.daporkchop.lib.encoding.Hexadecimal;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.encoding.compression.CompressionHelper;
import net.daporkchop.lib.primitive.map.ByteObjMap;
import net.daporkchop.lib.primitive.map.ObjByteMap;
import net.daporkchop.lib.primitive.map.hash.open.ByteObjOpenHashMap;
import net.daporkchop.lib.primitive.map.hash.open.ObjByteOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * A highly optimized rewrite of Mojang's original {@link RegionFile}, designed with backwards-compatibility in mind.
 * <p>
 * Of course, compression modes other than 1 and 2 (GZip and DEFLATE respectively) won't work with a vanilla Minecraft implementation, so
 * they should be avoided.
 *
 * @author DaPorkchop_
 */
public class OverclockedRegionFile implements AutoCloseable {
    protected static final int CHUNK_HEADER_SIZE = 5;
    protected static final int SECTOR_BYTES      = 4096;
    protected static final int SECTOR_INTS       = SECTOR_BYTES >> 2;

    public static final byte ID_NONE    = 0;
    public static final byte ID_GZIP    = 1; //official, no longer used by vanilla
    public static final byte ID_DEFLATE = 2; //official
    public static final byte ID_BZIP2   = 3;
    public static final byte ID_LZ4     = 4;
    public static final byte ID_LZMA    = 5;
    public static final byte ID_XZ      = 6;

    protected static final ByteObjMap<CompressionHelper> COMPRESSION_IDS         = new ByteObjOpenHashMap<>();
    protected static final ObjByteMap<CompressionHelper> REVERSE_COMPRESSION_IDS = new ObjByteOpenHashMap<>();
    protected static final byte[]                        EMPTY_SECTOR            = new byte[SECTOR_BYTES];

    static {
        //id => compression algo
        COMPRESSION_IDS.put(ID_NONE, Compression.NONE);
        COMPRESSION_IDS.put(ID_GZIP, Compression.GZIP_NORMAL);
        COMPRESSION_IDS.put(ID_DEFLATE, Compression.DEFLATE_NORMAL);
        COMPRESSION_IDS.put(ID_BZIP2, Compression.BZIP2_NORMAL);
        COMPRESSION_IDS.put(ID_LZ4, Compression.LZ4_BLOCK);
        COMPRESSION_IDS.put(ID_LZMA, Compression.LZMA_NORMAL);
        COMPRESSION_IDS.put(ID_XZ, Compression.XZ_NORMAL);

        //compression algo => id
        REVERSE_COMPRESSION_IDS.put(Compression.NONE, ID_NONE);
        REVERSE_COMPRESSION_IDS.put(Compression.GZIP_LOW, ID_GZIP);
        REVERSE_COMPRESSION_IDS.put(Compression.GZIP_NORMAL, ID_GZIP);
        REVERSE_COMPRESSION_IDS.put(Compression.GZIP_HIGH, ID_GZIP);
        REVERSE_COMPRESSION_IDS.put(Compression.DEFLATE_LOW, ID_DEFLATE);
        REVERSE_COMPRESSION_IDS.put(Compression.DEFLATE_NORMAL, ID_DEFLATE);
        REVERSE_COMPRESSION_IDS.put(Compression.DEFLATE_HIGH, ID_DEFLATE);
        REVERSE_COMPRESSION_IDS.put(Compression.BZIP2_LOW, ID_BZIP2);
        REVERSE_COMPRESSION_IDS.put(Compression.BZIP2_NORMAL, ID_BZIP2);
        REVERSE_COMPRESSION_IDS.put(Compression.BZIP2_HIGH, ID_BZIP2);
        REVERSE_COMPRESSION_IDS.put(Compression.LZ4_BLOCK, ID_LZ4);
        REVERSE_COMPRESSION_IDS.put(Compression.LZ4_FRAMED_64KB, ID_LZ4);
        REVERSE_COMPRESSION_IDS.put(Compression.LZ4_FRAMED_256KB, ID_LZ4);
        REVERSE_COMPRESSION_IDS.put(Compression.LZ4_FRAMED_1MB, ID_LZ4);
        REVERSE_COMPRESSION_IDS.put(Compression.LZ4_FRAMED_4MB, ID_LZ4);
        REVERSE_COMPRESSION_IDS.put(Compression.LZMA_LOW, ID_LZMA);
        REVERSE_COMPRESSION_IDS.put(Compression.LZMA_NORMAL, ID_LZMA);
        REVERSE_COMPRESSION_IDS.put(Compression.LZMA_HIGH, ID_LZMA);
        REVERSE_COMPRESSION_IDS.put(Compression.XZ_LOW, ID_XZ);
        REVERSE_COMPRESSION_IDS.put(Compression.XZ_NORMAL, ID_XZ);
        REVERSE_COMPRESSION_IDS.put(Compression.XZ_HIGH, ID_XZ);
    }

    private static void ensureInBounds(int x, int z) {
        if (x < 0 || x >= 32 || z < 0 || z >= 32) {
            throw new IllegalArgumentException(String.format("Coordinates out of bounds: (%d,%d)", x, z));
        }
    }

    @Getter
    protected final long             lastModified;
    protected final FileChannel      channel;
    protected final MappedByteBuffer index;
    protected final SparseBitSet  occupiedSectors = new SparseBitSet();
    protected final ReadWriteLock lock            = new ReentrantReadWriteLock();
    protected final boolean readOnly;

    public OverclockedRegionFile(@NonNull File file) throws IOException {
        PFiles.ensureDirectoryExists(file.getParentFile());

        {
            Path path = file.toPath();
            FileChannel channel;
            boolean readOnly = false;
            try {
                channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException e) {
                //try opening the file read-only
                //if this fails, then we don't have any filesystem access and won't be able to do anything about it, so the exception is thrown
                channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ);
                readOnly = true;
            }

            this.channel = channel;
            this.readOnly = readOnly;
            this.lastModified = file.lastModified();
        }

        long fileSize = this.channel.size();
        if (fileSize < SECTOR_BYTES) {
            //write the chunk offset table
            this.channel.write(ByteBuffer.wrap(EMPTY_SECTOR), 0L);
            //write another sector for the timestamp info
            this.channel.write(ByteBuffer.wrap(EMPTY_SECTOR), SECTOR_BYTES);
            //fileSize = SECTOR_BYTES << 1;
        } else if ((fileSize & 0xFFFL) != 0) {
            //the file size is not a multiple of 4KB, grow it
            this.channel.write(ByteBuffer.wrap(EMPTY_SECTOR, 0, (int) (fileSize & 0xFFFL)), fileSize);
        }

        this.index = this.channel.map(this.readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, SECTOR_BYTES);

        this.occupiedSectors.set(0); //chunk offset table
        this.occupiedSectors.set(1); //last modified info
        //init occupied sectors bitset
        for (int i = SECTOR_INTS - 1; i >= 0; i--) {
            int offset = this.index.getInt(i << 2);
            if (offset != 0) {
                this.occupiedSectors.set(offset >> 8, (offset >> 8) + (offset & 0xFF));
            }
        }

        System.out.println(this.occupiedSectors);
    }

    /**
     * Gets an {@link InputStream} that can inflate and read the contents of the chunk at the given coordinates (relative to this region).
     *
     * @param x the chunk's X coordinate
     * @param z the chunk's Z coordinate
     * @return an {@link InputStream} that can inflate and read the contents of the chunk at the given coordinates, or {@code null} if the chunk is not present
     * @throws IOException if an IO exception occurs you dummy
     */
    public InputStream read(int x, int z) throws IOException {
        ByteBuf buffer = this.readDirect(x, z);
        if (buffer == null) {
            return null;
        }
        byte compressionId = buffer.get();
        if (compressionId == ID_NONE) {
            return DataIn.wrap(buffer);
        } else {
            return COMPRESSION_IDS.get(compressionId).inflate(DataIn.wrapAsStream(buffer));
        }
    }

    /**
     * Reads the raw contents of the chunk at the given coordinates into a buffer.
     * <p>
     * Note that the buffer returned by this method should not be kept around! If you need to read multiple chunks at the same time, it is advised to use
     * your own buffer and {@link #readDirect(int, int, ByteBuffer)}.
     *
     * @param x the chunk's X coordinate
     * @param z the chunk's Z coordinate
     * @return a buffer containing the raw contents of the chunk, or {@code null} if the chunk is not present
     * @throws IOException if an IO exception occurs you dummy
     */
    public ByteBuf readDirect(int x, int z) throws IOException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.ioBuffer();
        //ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocateDirect(1 << 20).clear();
        return this.readDirect(x, z, buffer) ? buffer : null;
    }

    /**
     * Reads the raw contents of the chunk at the given coordinates into the given buffer.
     *
     * @param x      the chunk's X coordinate
     * @param z      the chunk's Z coordinate
     * @param buffer the buffer to read the chunk contents into
     * @return {@code true} if the chunk exists and could successfully be read into the buffer, {@code false} otherwise
     * @throws IOException if an IO exception occurs you dummy
     */
    public boolean readDirect(int x, int z, @NonNull ByteBuf buffer) throws IOException {
        ensureInBounds(x, z);

        this.lock.readLock().lock();
        try {
            int offset = this.getOffset(x, z);
            if (offset == 0) {
                return false;
            }

            System.out.printf("Sector: %d, %d sectors\n", offset >> 8, offset & 0xFF);

            int bytesToRead = (offset & 0xFF) * SECTOR_BYTES;
            buffer.setBytes()
            buffer.limit();
            this.channel.read(buffer, (offset >> 8) * SECTOR_BYTES);
            if (buffer.hasRemaining()) {
                throw new IOException(String.format("Couldn't read whole chunk! %d bytes remaining.", buffer.remaining()));
            }
            buffer.rewind().limit(buffer.getInt() + 4);
            return true;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * Gets a {@link DataOut} that will compress data written to it using the given compression type. The compressed data will be written to disk at the specified
     * region-local chunk coordinates when the {@link DataOut} instance is closed (using {@link DataOut#close()}.
     *
     * @param x           the chunk's X coordinate
     * @param z           the chunk's Z coordinate
     * @param compression the type of compression to use
     * @return a {@link DataOut} for writing data to the given chunk
     * @throws IOException if an IO exception occurs you dummy
     */
    public DataOut write(int x, int z, @NonNull CompressionHelper compression) throws IOException {
        byte compressionId = REVERSE_COMPRESSION_IDS.get(compression);
        if (compressionId == -1) {
            throw new IllegalArgumentException(String.format("Unregistered compression format: %s", compression));
        } else {
            return this.write(x, z, compression, compressionId);
        }
    }

    /**
     * Gets a {@link DataOut} that will compress data written to it using the given compression type. The compressed data will be written to disk at the specified
     * region-local chunk coordinates when the {@link DataOut} instance is closed (using {@link DataOut#close()}.
     *
     * @param x             the chunk's X coordinate
     * @param z             the chunk's Z coordinate
     * @param compression   the type of compression to use
     * @param compressionId the compression's ID, for writing to disk for decompression
     * @return a {@link DataOut} for writing data to the given chunk
     * @throws IOException if an IO exception occurs you dummy
     */
    public DataOut write(int x, int z, @NonNull CompressionHelper compression, byte compressionId) throws IOException {
        ensureInBounds(x, z);

        return new DataOut() {
            private final ByteBuffer buf = ((ByteBuffer) BUFFER_CACHE.get().clear().position(4)).put((byte) compressionId);
            private final OutputStream delegate = compression.deflate(DataOut.wrap(buf));

            @Override
            public void write(int b) throws IOException {
                this.delegate.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                this.delegate.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                this.delegate.close();

                OverclockedRegionFile.this.doWrite(x, z, buf);
            }
        };
    }

    /**
     * Writes raw chunk data to the region at the given region-local coordinates.
     * @param x             the chunk's X coordinate
     * @param z             the chunk's Z coordinate
     * @param b a {@code byte[]} containing the raw chunk data.
     * @throws IOException if an IO exception occurs you dummy
     */
    public void writeDirect(int x, int z, @NonNull byte[] b) throws IOException {
        this.writeDirect(x, z, ByteBuffer.wrap(b));
    }

    public void writeDirect(int x, int z, @NonNull ByteBuffer b) throws IOException {
        ensureInBounds(x, z);

        this.doWrite(x, z, ((ByteBuffer) BUFFER_CACHE.get().clear()).putInt(b.remaining()).put(b));
    }

    private void doWrite(int x, int z, @NonNull ByteBuffer buf) throws IOException {
        this.lock.writeLock().lock();
        try {
            buf.flip().mark();
            int remaining = buf.remaining();
            buf.putInt(remaining - 4).reset();

            int requiredSectors = (remaining + CHUNK_HEADER_SIZE) / SECTOR_BYTES + 1;
            int offset = this.getOffset(x, z);
            if (offset != 0) {
                if ((offset & 0xFF) == requiredSectors) {
                    //re-use old sectors
                    this.channel.write(buf, (offset >> 8) * SECTOR_BYTES);
                    return;
                } else {
                    //clear old sectors to search for new ones
                    //this makes it faster and less prone to bugs than shrinking existing allocations
                    this.occupiedSectors.clear(offset >> 8, (offset >> 8) + (offset & 0xFF));
                }
            }
            offset = 0;
            int i = this.occupiedSectors.nextClearBit(0);
            SEARCH:
            while (offset == 0) {
                int j = 0;
                while (j < requiredSectors) {
                    if (this.occupiedSectors.get(i + j++)) {
                        i = this.occupiedSectors.nextClearBit(i + j);
                        continue SEARCH;
                    }
                }
                //if we get this far, we've found a sufficiently long clear stretch
                offset = requiredSectors | (i << 8);
            }
            this.channel.write(buf, (offset >> 8) * SECTOR_BYTES);
            this.setOffset(x, z, offset);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean hasChunk(int x, int z) {
        return this.getOffset(x, z) != 0;
    }

    private int getOffset(int x, int z) {
        return this.index.getInt((x + z * 32) * 4);
    }

    private void setOffset(int x, int z, int offset) {
        this.index.putInt((x + z * 32) * 4, offset);
    }

    @Override
    public void close() throws IOException {
        this.index.force();
        PorkUtil.release(this.index);
        this.channel.close();
    }
}

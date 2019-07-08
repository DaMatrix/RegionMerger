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
import lombok.Getter;
import lombok.NonNull;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.encoding.compression.CompressionHelper;
import net.daporkchop.lib.primitive.map.ByteObjMap;
import net.daporkchop.lib.primitive.map.hash.open.ByteObjOpenHashMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
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
    protected static final ByteObjMap<CompressionHelper> COMPRESSION_IDS   = new ByteObjOpenHashMap<>();
    protected static final int                           CHUNK_HEADER_SIZE = 5;
    protected static final int                           SECTOR_BYTES      = 4096;
    protected static final int                           SECTOR_INTS       = SECTOR_BYTES >> 2;
    protected static final byte                          EMPTY_SECTOR[]    = new byte[SECTOR_BYTES];

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

    public InputStream readData(int x, int z) throws IOException {
        ensureInBounds(x, z);
        int offset = this.getOffset(x, z);
        if (offset == 0) {
            return null;
        }

        return null;
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

        return null;
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
    }
}

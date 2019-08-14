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

/*
 * Please disregard the license header above, I can't make my IDE exclude specific files from that.
 *
 * - DaPorkchop_
 */

/*
 * 2011 February 16
 *
 * This source code is based on the work of Scaevolus (see notice above).
 * It has been slightly modified by Mojang AB (constants instead of magic
 * numbers, a chunk timestamp header, and auto-formatted according to our
 * formatter template).
 */

package net.daporkchop.regionmerger.anvil;

import com.zaxxer.sparsebits.SparseBitSet;
import net.daporkchop.lib.common.function.io.IOFunction;
import net.daporkchop.lib.encoding.compression.Compression;
import net.daporkchop.lib.primitive.map.IntObjMap;
import net.daporkchop.lib.primitive.map.hash.open.IntObjOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

import static net.daporkchop.regionmerger.anvil.RegionConstants.*;

/**
 * Region File Format
 * <p>
 * Concept: The minimum unit of storage on hard drives is 4KB. 90% of Minecraft
 * chunks are smaller than 4KB. 99% are smaller than 8KB. Write a simple
 * container to store chunks in single files in runs of 4KB sectors.
 * <p>
 * Each region file represents a 32x32 group of chunks. The conversion from
 * chunk number to region number is floor(coord / 32): a chunk at (30, -3)
 * would be in region (0, -1), and one at (70, -30) would be at (3, -1).
 * Region files are named "r.x.z.data", where x and z are the region coordinates.
 * <p>
 * A region file begins with a 4KB header that describes where chunks are stored
 * in the file. A 4-byte big-endian integer represents sector offsets and sector
 * counts. The chunk offset for a chunk (x, z) begins at byte 4*(x+z*32) in the
 * file. The bottom byte of the chunk offset indicates the number of sectors the
 * chunk takes up, and the top 3 bytes represent the sector number of the chunk.
 * Given a chunk offset o, the chunk data begins at byte 4096*(o/256) and takes up
 * at most 4096*(o%256) bytes. A chunk cannot exceed 1MB in size. If a chunk
 * offset is 0, the corresponding chunk is not stored in the region file.
 * <p>
 * Chunk data begins with a 4-byte big-endian integer representing the chunk data
 * length in bytes, not counting the length field. The length must be smaller than
 * 4096 times the number of sectors. The next byte is a version field, to allow
 * backwards-compatible updates to how chunks are encoded.
 * <p>
 * A version of 1 represents a gzipped NBT file. The gzipped data is the chunk
 * length - 1.
 * <p>
 * A version of 2 represents a deflated (zlib compressed) NBT file. The deflated
 * data is the chunk length - 1.
 *
 * @author Scaevolus
 * @author Somebody at Mojang, probably Notch
 * @author Modified a bit by DaPorkchop_
 * @deprecated in favor of {@link OverclockedRegionFile}
 */
public interface RegionFile extends AutoCloseable {
    @Override
    void close() throws IOException;
}

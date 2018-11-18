import com.flowpowered.nbt.CompoundTag;
import com.flowpowered.nbt.Tag;
import com.flowpowered.nbt.stream.NBTInputStream;
import com.flowpowered.nbt.stream.NBTOutputStream;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileOutputStream;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.anvil.pork.PorkAnvilArchive;
import net.daporkchop.regionmerger.anvil.pork.ToPorkRegionConverter;
import net.daporkchop.regionmerger.util.IOEFunction;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.ThrowingConsumer;
import org.junit.Test;
import org.tukaani.xz.*;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author DaPorkchop_
 */
public class PorkianWorldEncoderThing {
    @Test
    public void encodeToCustomXZFormat() throws IOException {
        World world = ToPorkRegionConverter.inWorld;
        RandomAccessFile raf;
        {
            File file = new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\porkarchive_v2_xz\\temp_1.xz");
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                if (!file.createNewFile()) {
                    throw new IllegalStateException(String.format("couldn't create file: %s", file.getAbsolutePath()));
                }
            } else {
                file.delete();
                file.createNewFile();
            }
            raf = new RandomAccessFile(file, "rw");
        }
        FileChannel channel = raf.getChannel();
        //MappedByteBuffer indexBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 0);
        OutputStream mainOut = new PorkAnvilArchive.BufferedChannelOutput(8388608 * 4, channel, 0L);
        try {
            DataOut out;
            XZOutputStream zeh_xzOutputStream = null;
            boolean mode = true;
            if (mode) {
                zeh_xzOutputStream = new XZOutputStream(mainOut, new LZMA2Options(6), XZ.CHECK_NONE);
                out = DataOut.wrap(zeh_xzOutputStream);
                //TODO: compression threading: read and re-encode on one thread, compress on another
            } else {
                out = DataOut.wrap(mainOut);
            }
            OutputStream wrapper;
            XZOutputStream xzOutputStream = zeh_xzOutputStream;
            {
                OutputStream theOut = mode ? zeh_xzOutputStream : mainOut;
                wrapper = new OutputStream() {
                    private final OutputStream s = theOut;

                    @Override
                    public void write(byte[] b) throws IOException {
                        this.s.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        this.s.write(b, off, len);
                    }

                    @Override
                    public void write(int b) throws IOException {
                        this.s.write(b);
                    }
                };
            }
            AtomicInteger counter = new AtomicInteger(1);
            ThreadLocal<CompoundTag[]> tagArrCache = ThreadLocal.withInitial(() -> new CompoundTag[16]);
            world.ownedRegions.stream()
                    .filter(pos -> ((pos.x % 8) | (pos.z % 8)) == 0)
                    .map(world::getActualFileForRegion)
                    .map((IOEFunction<File, OverclockedRegionFile>) OverclockedRegionFile::new)
                    .forEach((ThrowingConsumer<OverclockedRegionFile, IOException>) inFile -> {
                        if (counter.getAndDecrement() <= 0) {
                            inFile.close();
                            return;
                        }
                        try {
                            //byte[] b = RegionMerger.BUFFER_CACHE.get();
                            CompoundTag[] tagArr = tagArrCache.get();
                            for (int x = 31; x >= 0; x--) {
                                for (int z = 31; z >= 0; z--) {
                                    if (!inFile.hasChunk(x, z)) {
                                        continue;
                                    }
                                    //int i = 0;
                                    Tag tag;
                                    try (NBTInputStream in = new NBTInputStream(inFile.readData(x, z), false)) {
                                        tag = in.readTag();
                                        //int j;
                                        //while ((j = in.read(b, i, b.length - i)) != -1) {
                                        //    i += j;
                                        //}
                                    } catch (Exception e) {
                                        continue;
                                    }
                                    {
                                        //ultimate optimizations!
                                        //remove chunks from NBT and serialize them directly
                                        //TODO
                                    }
                                    //out.writeVarInt(i, true);
                                    out.writeBoolean(true);
                                    out.write(x);
                                    out.write(z);
                                    //out.write(b);
                                    try (NBTOutputStream stream = new NBTOutputStream(wrapper, false)) {
                                        stream.writeTag(tag);
                                    }
                                    if (xzOutputStream != null) {
                                        //xzOutputStream.endBlock();
                                    }
                                    for (int y = 15; y >= 0; y--) {
                                        tagArr[y] = null;
                                    }
                                }
                            }
                        } finally {
                            inFile.close();
                        }
                    });
            //out.writeVarInt(0, true); //length of zero indicates end of stream
            out.writeBoolean(false);
            out.close();
        } finally {
            //channel.close();
            raf.close();
            System.out.println("Done!");
        }
    }

    @Test
    public void decodeItAgain() throws IOException {
        File file = new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\porkarchive_v2_xz\\temp_1.xz");
        SeekableXZInputStream xzInputStream = new SeekableXZInputStream(new SeekableFileInputStream(file));
        DataIn in = DataIn.wrap(xzInputStream);
    }
}

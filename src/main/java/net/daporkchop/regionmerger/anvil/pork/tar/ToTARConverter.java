package net.daporkchop.regionmerger.anvil.pork.tar;

import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileOutputStream;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.anvil.pork.ToPorkRegionConverter;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.ThrowingConsumer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author DaPorkchop_
 */
public class ToTARConverter {
    public static void main(String... args) {
        World inWorld = ToPorkRegionConverter.inWorld;
        TFile outFile = new TFile(String.format("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\porkarchive_v3_tar\\out_%d.tar", System.currentTimeMillis()));
        AtomicBoolean running = new AtomicBoolean(true);
        {
            Thread t = new Thread(() -> {
                try (Scanner scanner = new Scanner(System.in)) {
                    scanner.nextLine();
                }
                running.set(false);
            });
            t.setDaemon(true);
            t.start();
        }
        AtomicInteger counter = new AtomicInteger(0);
        inWorld.ownedRegions
                //.stream().map(inWorld::getActualFileForRegion)
                .forEach((ThrowingConsumer<Pos, IOException>) pos -> {
                    if (!running.get()) {
                        return;
                    }
                    byte[] b = RegionMerger.BUFFER_CACHE.get();
                    try (OverclockedRegionFile inFile = new OverclockedRegionFile(inWorld.getActualFileForRegion(pos));
                         OutputStream out = new BufferedOutputStream(new TFileOutputStream(new TFile(outFile, String.valueOf(counter.getAndIncrement()))), 8192 * 4)) {
                        DataOut dataOut = DataOut.wrap(out);
                        //TODO: write region coords
                        dataOut.writeVarInt(pos.x);
                        dataOut.writeVarInt(pos.z);
                        for (int x = 31; running.get() && x >= 0; x--) {
                            for (int z = 31; running.get() && z >= 0; z--) {
                                if (inFile.hasChunk(x, z)) {
                                    /*try (NBTInputStream in = new NBTInputStream(inFile.readData(x, z), false)) {
                                        CompoundTag tag = (CompoundTag) in.readTag();
                                        //TODO: optimizations
                                        dataOut.writeBoolean(true);
                                        dataOut.write(x);
                                        dataOut.write(z);
                                        nbtOut.writeTag(tag);
                                    }*/
                                    dataOut.writeBoolean(true);
                                    dataOut.write(x);
                                    dataOut.write(z);
                                    int j = 0;
                                    try (InputStream in = inFile.readData(x, z))    {
                                        int i;
                                        while ((i = in.read(b, j, b.length - j)) != -1)  {
                                            j += i;
                                        }
                                    }
                                    dataOut.writeVarInt(j, true);
                                    out.write(b, 0, j);
                                }
                            }
                        }
                        dataOut.writeBoolean(false);
                    }
                });
    }
}

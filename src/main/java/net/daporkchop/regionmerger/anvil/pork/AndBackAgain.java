package net.daporkchop.regionmerger.anvil.pork;

import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.anvil.mojang.RegionFile;
import net.daporkchop.regionmerger.util.IOEFunction;
import net.daporkchop.regionmerger.util.ThrowingConsumer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author DaPorkchop_
 */
public class AndBackAgain {
    public static void main(String... args) {
        World worldIn = new World(new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\Spawn_PorkAnvil_v1_Uncompressed_(Default)"), "pav1", false);
        worldIn.ownedRegions.stream()
                .map(worldIn::getActualFileForRegion)
                .map((IOEFunction<File, PorkRegion>) PorkRegion::new)
                .forEach((ThrowingConsumer<PorkRegion, IOException>) regionIn -> {
                    System.out.printf("Converting %s...\n", regionIn.getFile().getAbsolutePath());
                    File file = regionIn.getFile();
                    RegionFile regionOut = new RegionFile(new File(file.getAbsolutePath().replace("pav1", "mca")));
                    try {
                        for (int x = 31; x >= 0; x--)   {
                            for (int z = 31; z >= 0; z--)   {
                                if(!regionIn.hasChunk(x, z))    {
                                    continue;
                                }
                                try (InputStream in = regionIn.read(x, z);
                                     OutputStream out = regionOut.getChunkDataOutputStream(x, z))  {
                                    byte[] b = RegionMerger.BUFFER_CACHE.get();
                                    int i;
                                    while ((i = in.read(b)) != -1)  {
                                        out.write(b, 0, i);
                                    }
                                }
                            }
                        }
                    } finally {
                        regionIn.close();
                        regionOut.close();
                    }
                    System.out.println("done ok");
                });
        System.out.println("Done!");
    }
}

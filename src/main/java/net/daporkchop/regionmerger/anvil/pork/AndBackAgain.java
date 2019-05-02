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

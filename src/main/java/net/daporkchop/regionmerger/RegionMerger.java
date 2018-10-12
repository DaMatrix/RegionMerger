package net.daporkchop.regionmerger;

import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.RegionFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class RegionMerger {
    public static void main(String... args) throws IOException {
        if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
            System.out.println("PorkRegionMerger v0.0.1");
            System.out.println("");
            System.out.println("--input=<path>       Add an input directory, must be a path to the root of a Minecraft save");
            System.out.println("--output=<path>      Set the output directory");
            System.out.println("--help               Show this help message");
            return;
        }
        Collection<File> inputDirs = new ArrayDeque<>();
        File outputDir = null;
        for (String s : args) {
            if (s.startsWith("--input=")) {
                File file = new File(s.split("=")[1]);
                if (!file.exists()) {
                    System.err.printf("Invalid input directory: %s\n", s.split("=")[1]);
                    return;
                }
                inputDirs.add(file);
            } else if (s.startsWith("--output=")) {
                File file = new File(s.split("=")[1]);
                if (!file.exists() && !file.mkdirs()) {
                    System.err.printf("Invalid output directory: %s\n", s.split("=")[1]);
                    return;
                }
                outputDir = file;
            } else {
                System.err.printf("Invalid argument: \"%s\"\n", s);
                return;
            }
        }
        if (inputDirs.isEmpty()) {
            System.err.println("No inputs specified!");
            return;
        } else if (outputDir == null) {
            System.err.println("No output specified!");
            return;
        }
        World outWorld = new World(outputDir);
        Collection<World> worlds = new ArrayDeque<>();
        Map<Pos, Collection<World>> poslookup = new Hashtable<>();
        inputDirs.forEach(f -> worlds.add(new World(f)));
        worlds.forEach(w -> w.ownedRegions.forEach(p -> poslookup.computeIfAbsent(p, x -> new ArrayDeque<>()).add(w)));
        System.out.printf("Loaded %d worlds with a total of %d different regions\n", worlds.size(), poslookup.size());
        Collection<RegionFile> regionFiles = new ArrayDeque<>();
        Collection<RegionFile> regionFilesChunk = new ArrayDeque<>();
        poslookup.forEach((pos, viableWorlds) -> {
            regionFiles.clear();
            viableWorlds.forEach(world -> regionFiles.add(world.getRegion(pos)));
            RegionFile out = outWorld.getRegion(pos);
            for (int x = 31; x >= 0; x--) {
                for (int z = 31; z >= 0; z--) {
                    regionFilesChunk.clear();
                    int x1 = x;
                    int z1 = z;
                    regionFiles.forEach(f -> {
                        if (f.hasChunk(x1, z1)) {
                            regionFilesChunk.add(f);
                        }
                    });
                    if (regionFilesChunk.isEmpty()) {
                        //System.out.printf("Warning: Chunk (%d,%d) in region (%d,%d) not found!\n", pos.x, pos.z, x, z);
                    } else {
                        Iterator<RegionFile> iterator = regionFilesChunk.stream().sorted(Comparator.comparingLong(RegionFile::lastModified)).iterator();
                        RegionFile r = iterator.next();
                        try (OutputStream os = out.getChunkDataOutputStream(x, z)) {
                            InputStream is = r.getChunkDataInputStream(x, z);
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = is.read(buffer)) != -1) {
                                os.write(buffer, 0, len);
                            }
                            is.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        });
        System.out.println("Done!");
    }
}

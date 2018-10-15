package net.daporkchop.regionmerger;

import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.RegionFile;
import net.daporkchop.regionmerger.util.ThrowingBiConsumer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RegionMerger {
    public static final ThreadLocal<byte[]> BUFFER_CACHE = ThreadLocal.withInitial(() -> new byte[4096]);

    public static void main(String... args) throws IOException {
        if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
            System.out.println("PorkRegionMerger v0.0.1");
            System.out.println();
            System.out.println("--help                  Show this help message");
            System.out.println("--input=<path>          Add an input directory, must be a path to the root of a Minecraft save");
            System.out.println("--output=<path>         Set the output directory");
            System.out.println("--mode=<mode>           Set the mode (defaults to merge)");
            System.out.println("--areaCenterX=<x>       Set the center of the area along the X axis (only for area mode) (in regions)");
            System.out.println("--areaCenterZ=<z>       Set the center of the area along the Z axis (only for area mode) (in regions)");
            System.out.println("--areaRadius=<r>        Set the radius of the area (only for area mode) (in regions)");
            System.out.println("--suppressAreaWarnings  Hide warnings for chunks with no inputs (only for area mode)");
            System.out.println();
            System.out.println("  Modes:  merge         Simply merges all chunks from all regions into the output");
            System.out.println("          area          Merges all chunks in a specified area into the output");
            return;
        }
        Collection<File> inputDirs = new ArrayDeque<>();
        File outputDir = null;
        String mode = "merge";
        AtomicInteger areaCenterX = new AtomicInteger(0);
        AtomicInteger areaCenterZ = new AtomicInteger(0);
        AtomicInteger areaRadius = new AtomicInteger(-1);
        AtomicBoolean suppressAreaWarnings = new AtomicBoolean(false);
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
            } else if (s.startsWith("--mode=")) {
                mode = s.split("=")[1];
                switch (mode) {
                    case "merge":
                    case "area":
                        break;
                    default: {
                        System.err.printf("Unknown mode: %s\n", mode);
                        return;
                    }
                }
            } else if (s.startsWith("--areaCenterX=")) {
                s = s.split("=")[1];
                try {
                    areaCenterX.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    System.err.printf("Invalid number: %s\n", s);
                    return;
                }
            } else if (s.startsWith("--areaCenterZ=")) {
                s = s.split("=")[1];
                try {
                    areaCenterZ.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    System.err.printf("Invalid number: %s\n", s);
                    return;
                }
            } else if (s.startsWith("--areaRadius=")) {
                s = s.split("=")[1];
                try {
                    areaRadius.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    System.err.printf("Invalid number: %s\n", s);
                    return;
                }
            } else if (s.startsWith("--suppressAreaWarnings=")) {
                suppressAreaWarnings.set(Boolean.parseBoolean(s.split("=")[1]));
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
        inputDirs.forEach(f -> worlds.add(new World(f)));
        Collection<RegionFile> regionFiles = new ArrayDeque<>();
        Collection<RegionFile> regionFilesChunk = new ArrayDeque<>();

        switch (mode) {
            case "merge": {
                Map<Pos, Collection<World>> poslookup = new Hashtable<>();
                worlds.forEach(w -> w.ownedRegions.forEach(p -> poslookup.computeIfAbsent(p, x -> new ArrayDeque<>()).add(w)));
                System.out.printf("Loaded %d worlds with a total of %d different regions\n", worlds.size(), poslookup.size());

                poslookup.forEach((ThrowingBiConsumer<? super Pos, ? super Collection<World>, IOException>) (pos, viableWorlds) -> {
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
                                OutputStream os = out.getChunkDataOutputStream(x, z);
                                InputStream is = r.getChunkDataInputStream(x, z);
                                byte[] buffer = BUFFER_CACHE.get();
                                int len;
                                while ((len = is.read(buffer)) != -1) {
                                    os.write(buffer, 0, len);
                                }
                                is.close();
                                os.close();
                            }
                        }
                    }
                    out.close();
                    for (RegionFile file : regionFiles) {
                        file.close();
                    }
                    regionFiles.clear();
                });
            }
            break;
            case "area": {
                int radius = areaRadius.get();
                int centerX = areaCenterX.get();
                int centerZ = areaCenterZ.get();
                if (radius <= 0) {
                    System.err.printf("Radius may not be less than or equal to 0! (Value: %d)\n", radius);
                    return;
                }
                centerX -= radius;
                centerZ -= radius;
                for (int X = radius << 1; X >= 0; X--) {
                    for (int Z = radius << 1; Z >= 0; Z--) {
                        int X1 = X - radius + centerX;
                        int Z1 = Z - radius + centerZ;
                        worlds.forEach(world -> regionFiles.add(world.getRegion(X1, Z1)));
                        RegionFile out = outWorld.getRegion(X1, Z1);
                        for (int x = 31; x >= 0; x--) {
                            for (int z = 31; z >= 0; z--) {
                                regionFilesChunk.clear();
                                //int x1 = ((X - radius + centerX) << 5) + x;
                                //int z1 = ((Z - radius + centerZ) << 5) + z;
                                int x1 = x;
                                int z1 = z;
                                regionFiles.forEach(f -> {
                                    if (f.hasChunk(x1, z1)) {
                                        regionFilesChunk.add(f);
                                    }
                                });
                                if (regionFilesChunk.isEmpty()) {
                                    if (!suppressAreaWarnings.get())    {
                                        System.out.printf("Warning: Chunk (%d,%d) in region (%d,%d) not found!\n", x, z, X1, Z1);
                                    }
                                } else {
                                    Iterator<RegionFile> iterator = regionFilesChunk.stream().sorted(Comparator.comparingLong(RegionFile::lastModified)).iterator();
                                    RegionFile r = iterator.next();
                                    OutputStream os = out.getChunkDataOutputStream(x, z);
                                    InputStream is = r.getChunkDataInputStream(x, z);
                                    byte[] buffer = BUFFER_CACHE.get();
                                    int len;
                                    while ((len = is.read(buffer)) != -1) {
                                        os.write(buffer, 0, len);
                                    }
                                    is.close();
                                    os.close();
                                }
                            }
                        }
                        out.close();
                        for (RegionFile file : regionFiles) {
                            file.close();
                        }
                        regionFiles.clear();
                    }
                }
            }
            break;
            default: {
                System.err.printf("Unknown mode: %s\n", mode);
                return;
            }
        }

        System.out.println("Done!");
    }
}

package net.daporkchop.regionmerger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import jdk.nashorn.internal.ir.debug.JSONWriter;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.RegionFile;
import net.daporkchop.regionmerger.util.ThrowingBiConsumer;
import net.daporkchop.regionmerger.util.ThrowingConsumer;

import java.io.*;
import java.nio.charset.Charset;
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
            System.out.println("          findmissing   Finds all chunks that aren't defined in an input and dumps them to a json file. Uses settings from area mode, however units are measured in chunks instead of regions.");
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
                    case "findmissing":
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
        } else if (outputDir == null && !"findmissing".equals(mode)) {
            System.err.println("No output specified!");
            return;
        }
        World outWorld = "findmissing".equals(mode) ? null : new World(outputDir);
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
                    RegionFile out = outWorld.getRegion(pos, false);
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
                                System.out.printf("Warning: Chunk (%d,%d) in region (%d,%d) not found!\n", pos.x, pos.z, x, z);
                            } else {
                                Iterator<RegionFile> iterator = regionFilesChunk.stream().sorted(Comparator.comparingLong(RegionFile::lastModified)).iterator();
                                RegionFile r = iterator.next();
                                OutputStream os = out.getChunkDataOutputStream(x, z);
                                InputStream is = r.getChunkDataInputStream(x, z);
                                if (is == null) {
                                    throw new NullPointerException(String.format("Illegal chunk (%d,%d) in region %s", x, z, r.fileName.getAbsolutePath()));
                                }
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
                centerX -= radius >> 1;
                centerZ -= radius >> 1;
                for (int X = radius; X >= 0; X--) {
                    for (int Z = radius; Z >= 0; Z--) {
                        int X1 = X/* - radius*/ + centerX;
                        int Z1 = Z/* - radius*/ + centerZ;
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
            case "findmissing": {
                if (worlds.size() != 1) {
                    System.err.printf("Mode 'findmissing' requires exactly one input, but found %d\n", worlds.size());
                    return;
                }
                Map<Pos, RegionFile> regionFileCache = new Hashtable<>();
                World world = ((ArrayDeque<World>) worlds).element();
                Set<Pos> missingPositions = new HashSet<>();

                int rad = areaRadius.get();
                int X = areaCenterX.get() - rad;
                int XX = X + (rad << 1);
                int Z = areaCenterZ.get() - rad;
                int ZZ = Z + (rad << 1);
                for (int x = X; x <= XX; x++)   {
                    for (int z = Z; z <= ZZ; z++)   {
                        RegionFile file = regionFileCache.computeIfAbsent(new Pos(x >> 5, z >> 5), world::getRegionOrNull);
                        if (file == null || !file.hasChunk(x & 0x1F, z & 0x1F))   {
                            if (!suppressAreaWarnings.get())    {
                                System.out.printf("Chunk (%d,%d) in region (%d,%d) not found!\n", x & 0x1F, z & 0x1F, x >> 5, z >> 5);
                            }
                            missingPositions.add(new Pos(x, z));
                        }
                    }
                }
                regionFileCache.values().forEach((ThrowingConsumer<RegionFile, IOException>) RegionFile::close);
                JsonArray array = new JsonArray();
                missingPositions.forEach(pos -> {
                    JsonObject object = new JsonObject();
                    object.addProperty("x", pos.x);
                    object.addProperty("z", pos.z);
                    array.add(object);
                });
                Gson gson = new GsonBuilder()
                        .setPrettyPrinting()
                        .create();
                byte[] json = gson.toJson(array).getBytes(Charset.forName("UTF-8"));
                try (OutputStream os = new FileOutputStream(new File(".", "missingchunks.json")))   {
                    os.write(json);
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

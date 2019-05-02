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

package net.daporkchop.regionmerger;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import net.daporkchop.lib.logging.Logging;
import net.daporkchop.regionmerger.anvil.mojang.RegionFile;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.ThrowingBiConsumer;
import net.daporkchop.regionmerger.util.ThrowingConsumer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RegionMerger implements Logging {
    public static final ThreadLocal<byte[]> BUFFER_CACHE = ThreadLocal.withInitial(() -> new byte[0xFFFFFF]);

    public static void main(String... args) throws IOException {
        logger.enableANSI();
        if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
            logger.channel("Help")
                  .info("PorkRegionMerger v0.0.7")
                  .info()
                  .info("--help                  Show this help message")
                  .info("--input=<path>          Add an input directory, must be a path to the root of a Minecraft save")
                  .info("--output=<path>         Set the output directory")
                  .info("--mode=<mode>           Set the mode (defaults to merge)")
                  .info("--areaCenterX=<x>       Set the center of the area along the X axis (only for area mode) (in chunks)")
                  .info("--areaCenterZ=<z>       Set the center of the area along the Z axis (only for area mode) (in chunks)")
                  .info("--areaRadius=<r>        Set the radius of the area (only for area mode) (in chunks)")
                  .info("--findMinX=<minX>       Set the min X coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMinZ=<minZ>       Set the min Z coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMaxX=<minX>       Set the max X coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--findMaxZ=<minZ>       Set the max Z coord to check (only for findmissing mode) (in regions) (inclusive)")
                  .info("--suppressAreaWarnings  Hide warnings for chunks with no inputs (only for area mode)")
                  .info("--skipincomplete        Skip incomplete regions (only for merge mode)")
                  .info("--forceoverwrite        Forces the merger to overwrite all currently present chunks (only for add mode)")
                  .info("--inputorder            Prioritize inputs by their input order rather than by file age (only for add mode)")
                  .info("--skipcopy              Slower, can fix some issues (only for merge mode)")
                  .info("--verbose   (-v)        Print more messages to your console (if you like spam ok)")
                  .info("-j=<threads>            The number of worker threads (only for add mode, defaults to cpu count)")
                  .info()
                  .info("  Modes:  merge         Simply merges all chunks from all regions into the output")
                  .info("          area          Merges all chunks in a specified area into the output")
                  .info("          findmissing   Finds all chunks that aren't defined in an input and dumps them to a json file. Uses settings from area mode.")
                  .info("          add           Add all the chunks from every input into the output world, without removing any");
            return;
        }
        Collection<File> inputDirs = new ArrayDeque<>();
        File outputDir = null;
        String mode = "merge";
        AtomicInteger areaCenterX = new AtomicInteger(0);
        AtomicInteger areaCenterZ = new AtomicInteger(0);
        AtomicInteger findMinX = new AtomicInteger(0);
        AtomicInteger findMinZ = new AtomicInteger(0);
        AtomicInteger findMaxX = new AtomicInteger(0);
        AtomicInteger findMaxZ = new AtomicInteger(0);
        AtomicInteger areaRadius = new AtomicInteger(-1);
        AtomicInteger threads = new AtomicInteger(Runtime.getRuntime().availableProcessors());
        AtomicBoolean suppressAreaWarnings = new AtomicBoolean(false);
        AtomicBoolean verbose = new AtomicBoolean(false);
        AtomicBoolean skipincomplete = new AtomicBoolean(false);
        AtomicBoolean forceoverwrite = new AtomicBoolean(false);
        AtomicBoolean inputorder = new AtomicBoolean(false);
        AtomicBoolean skipcopy = new AtomicBoolean(false);
        for (String s : args) {
            if (s.startsWith("--input=")) {
                File file = new File(s.split("=")[1]);
                if (!file.exists()) {
                    logger.error("Invalid input directory: %s", s.split("=")[1]);
                    return;
                }
                inputDirs.add(file);
            } else if (s.startsWith("--output=")) {
                File file = new File(s.split("=")[1]);
                if (!file.exists() && !file.mkdirs()) {
                    logger.error("Invalid output directory: %s", s.split("=")[1]);
                    return;
                }
                outputDir = file;
            } else if (s.startsWith("--mode=")) {
                mode = s.split("=")[1];
                switch (mode) {
                    case "merge":
                    case "area":
                    case "findmissing":
                    case "add":
                    case "removeoutlying":
                        break;
                    default: {
                        logger.error("Unknown mode: %s", mode);
                        return;
                    }
                }
            } else if (s.startsWith("--findMinX=")) {
                s = s.split("=")[1];
                try {
                    findMinX.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--findMinZ=")) {
                s = s.split("=")[1];
                try {
                    findMinZ.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--findMaxX=")) {
                s = s.split("=")[1];
                try {
                    findMaxX.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--findMaxZ=")) {
                s = s.split("=")[1];
                try {
                    findMaxZ.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--areaCenterX=")) {
                s = s.split("=")[1];
                try {
                    areaCenterX.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--areaCenterZ=")) {
                s = s.split("=")[1];
                try {
                    areaCenterZ.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("--areaRadius=")) {
                s = s.split("=")[1];
                try {
                    areaRadius.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if (s.startsWith("-j=")) {
                s = s.split("=")[1];
                try {
                    threads.set(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    logger.error("Invalid number: %s", s);
                    return;
                }
            } else if ("--suppressAreaWarnings".equals(s)) {
                suppressAreaWarnings.set(true);
            } else if ("--verbose".equals(s) || "-v".equals(s)) {
                verbose.set(true);
            } else if ("--skipincomplete".equals(s)) {
                skipincomplete.set(true);
            } else if ("--forceoverwrite".equals(s)) {
                forceoverwrite.set(true);
            } else if ("--inputorder".equals(s)) {
                inputorder.set(true);
            } else if ("--skipcopy".equals(s)) {
                skipcopy.set(true);
            } else {
                logger.error("Invalid argument: \"%s\"", s);
                return;
            }
        }
        if (inputDirs.isEmpty()) {
            logger.error("No inputs specified!");
            return;
        } else if (outputDir == null && !("findmissing".equals(mode) || "removeoutlying".equals(mode))) {
            logger.error("No output specified!");
            return;
        }
        World outWorld = ("findmissing".equals(mode) || "removeoutlying".equals(mode)) ? null : new World(outputDir);
        Collection<World> worlds = new ArrayDeque<>();
        inputDirs.forEach(f -> worlds.add(new World(f)));
        Collection<RegionFile> regionFiles = new ArrayDeque<>();
        Collection<RegionFile> regionFilesChunk = new ArrayDeque<>();

        switch (mode) {
            case "merge": {
                AtomicBoolean running = new AtomicBoolean(true);
                AtomicInteger totalRegions = new AtomicInteger(0);
                AtomicInteger currentRegion = new AtomicInteger(0);
                Map<Pos, Collection<World>> poslookup = new Hashtable<>();
                worlds.forEach(w -> w.ownedRegions.forEach(p -> {
                    poslookup.computeIfAbsent(p, x -> new ArrayDeque<>()).add(w);
                }));
                logger.info("Loaded %d worlds with a total of %d different regions", worlds.size(), poslookup.size());

                totalRegions.set(poslookup.size());
                if (verbose.get()) {
                    new Thread(() -> {
                        while (running.get()) {
                            logger.info("Current progess: copying region %d/%d", currentRegion.get(), totalRegions.get());
                            try {
                                Thread.sleep(2000L);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }).start();
                }

                poslookup.forEach((ThrowingBiConsumer<? super Pos, ? super Collection<World>, IOException>) (pos, viableWorlds) -> {
                    viableWorlds.forEach(world -> regionFiles.add(world.getRegion(pos)));
                    RegionFile out = outWorld.getRegion(pos, false);
                    RUN:
                    {
                        if (!skipcopy.get()) {
                            FILELOOP:
                            for (RegionFile file : regionFiles) {
                                for (int x = 31; x >= 0; x--) {
                                    for (int z = 31; z >= 0; z--) {
                                        if (!file.hasChunk(x, z)) {
                                            continue FILELOOP;
                                        }
                                    }
                                }
                                //if we've gotten here, the region has all chunks and we can copy it directly
                                out.close();
                                OutputStream os = new FileOutputStream(out.fileName);
                                InputStream is = new FileInputStream(file.fileName);
                                byte[] buffer = BUFFER_CACHE.get();
                                int len;
                                while ((len = is.read(buffer)) != -1) {
                                    os.write(buffer, 0, len);
                                }
                                is.close();
                                os.close();
                                break RUN;
                            }
                        }
                        if (skipincomplete.get()) {
                            break RUN;
                        }
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
                                    logger.trace("Chunk (%d,%d) in region (%d,%d) not found!", x, z, pos.x, pos.z);
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
                    }
                    for (RegionFile file : regionFiles) {
                        file.close();
                    }
                    regionFiles.clear();
                    currentRegion.incrementAndGet();
                });

                running.set(false);
            }
            break;
            case "area": {
                Map<Pos, Collection<RegionFile>> inputFileCache = new Hashtable<>();
                Map<Pos, AtomicInteger> inputFileDependencies = new Hashtable<>();
                Map<Pos, RegionFile> outputFileCache = new Hashtable<>();

                int rad = areaRadius.get();
                int X = areaCenterX.get() - rad;
                int XX = X + (rad << 1);
                int Z = areaCenterZ.get() - rad;
                int ZZ = Z + (rad << 1);

                AtomicBoolean running = new AtomicBoolean(true);
                AtomicInteger totalChunks = new AtomicInteger(0);
                AtomicInteger currentChunk = new AtomicInteger(0);
                for (int x = X; x <= XX; x++) {
                    for (int z = Z; z <= ZZ; z++) {
                        totalChunks.incrementAndGet();
                        Pos p = new Pos(x >> 5, z >> 5);
                        inputFileCache.computeIfAbsent(p, pos -> worlds.stream()
                                                                       .filter(w -> w.ownedRegions.contains(pos))
                                                                       .map(w -> w.getRegion(pos))
                                                                       .sorted(Comparator.comparingLong(RegionFile::lastModified))
                                                                       .collect(Collectors.toCollection(ArrayDeque::new)));
                        inputFileDependencies.computeIfAbsent(p, pos -> new AtomicInteger(0)).incrementAndGet();
                    }
                }
                long startTime = System.currentTimeMillis();
                long totalSize = 0L;
                logger.info("Copying %d chunks...", totalChunks.get());

                if (verbose.get()) {
                    new Thread(() -> {
                        while (running.get()) {
                            logger.info("Current progess: copying chunk %d/%d", currentChunk.get(), totalChunks.get());
                            try {
                                Thread.sleep(2000L);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }).start();
                }

                for (int x = X; x <= XX; x++) {
                    for (int z = Z; z <= ZZ; z++) {
                        Pos regionPos = new Pos(x >> 5, z >> 5);
                        Pos chunkPos = new Pos(x & 0x1F, z & 0x1F);
                        RegionFile file = null;
                        for (RegionFile rf : inputFileCache.get(regionPos)) {
                            if (file == null && rf.hasChunk(chunkPos.x, chunkPos.z)) {
                                file = rf;
                            }
                        }
                        if (file == null || !file.hasChunk(chunkPos.x, chunkPos.z)) {
                            logger.warn("Chunk (%02d,%02d) in region (%04d,%04d) not found!", chunkPos.x, chunkPos.z, regionPos.x, regionPos.z);
                        } else {
                            RegionFile outputFile = outputFileCache.computeIfAbsent(regionPos, outWorld::getOrCreateRegion);
                            OutputStream os = outputFile.getChunkDataOutputStream(chunkPos.x, chunkPos.z);
                            InputStream is = file.getChunkDataInputStream(chunkPos.x, chunkPos.z);
                            if (is == null) {
                                throw new NullPointerException(String.format("Illegal chunk (%02d,%02d) in region %s", chunkPos.x, chunkPos.z, file.fileName.getAbsolutePath()));
                            }
                            byte[] buffer = BUFFER_CACHE.get();
                            int len;
                            while ((len = is.read(buffer)) != -1) {
                                os.write(buffer, 0, len);
                                totalSize += len;
                            }
                            is.close();
                            os.close();

                            currentChunk.incrementAndGet();
                            if (false) {
                                logger.trace("Copied chunk (%02d,%02d) in region (%04d,%04d)    %d/%d", chunkPos.x, chunkPos.z, regionPos.x, regionPos.z, currentChunk.get(), totalChunks.get());
                            }
                        }
                        if (inputFileDependencies.get(regionPos).decrementAndGet() <= 0) {
                            logger.trace("Removing all open files for region (%04d,%04d)", regionPos.x, regionPos.z);
                            inputFileCache.remove(regionPos).forEach((ThrowingConsumer<RegionFile, IOException>) RegionFile::close);
                        }
                    }
                }

                running.set(false);

                logger.info("Closing files...");
                outputFileCache.values().forEach((ThrowingConsumer<RegionFile, IOException>) RegionFile::close);
                inputFileCache.values().forEach(c -> c.forEach((ThrowingConsumer<RegionFile, IOException>) RegionFile::close));

                startTime = System.currentTimeMillis() - startTime;
                logger.success("Copied %d chunks (%d bytes, %.2f MB) in %dh:%dm:%ds", totalChunks.get(), totalSize, (float) totalSize / (1024.0f * 1024.0f), startTime / (1000L * 60L * 60L), startTime / (1000L * 60L) % 60, startTime / 1000L % 60);
            }
            break;
            case "findmissing": {
                if (worlds.size() != 1) {
                    logger.error("Mode 'findmissing' requires exactly one input, but found %d", worlds.size());
                    return;
                }
                World world = ((ArrayDeque<World>) worlds).element();
                Set<Pos> missingPositions = new HashSet<>();

                for (int x = findMinX.get(); x <= findMaxX.get(); x++) {
                    for (int z = findMinZ.get(); z <= findMaxZ.get(); z++) {
                        RegionFile file = world.getRegionOrNull(new Pos(x, z));
                        if (file == null) {
                            for (int xx = 31; xx >= 0; xx--) {
                                for (int zz = 31; zz >= 0; zz--) {
                                    missingPositions.add(new Pos((x << 5) + xx, (z << 5) + zz));
                                }
                            }
                        } else {
                            file.close();
                            for (int xx = 31; xx >= 0; xx--) {
                                for (int zz = 31; zz >= 0; zz--) {
                                    if (!file.hasChunk(xx, zz)) {
                                        missingPositions.add(new Pos((x << 5) + xx, (z << 5) + zz));
                                    }
                                }
                            }
                        }
                    }
                }
                JsonArray array = new JsonArray();
                missingPositions.stream().map(pos -> {
                    JsonObject object = new JsonObject();
                    object.addProperty("x", pos.x);
                    object.addProperty("z", pos.z);
                    return object;
                }).forEach(array::add);
                byte[] json = new GsonBuilder()
                        //.setPrettyPrinting()
                        .create().toJson(array).getBytes(Charset.forName("UTF-8"));
                try (OutputStream os = new FileOutputStream(new File(".", "missingchunks.json"))) {
                    os.write(json);
                }
            }
            break;
            case "add": {
                long time = System.currentTimeMillis();
                AtomicLong totalSize = new AtomicLong(0L);
                AtomicInteger totalRegions = new AtomicInteger(0);
                AtomicInteger currentRegion = new AtomicInteger(0);
                AtomicBoolean running = new AtomicBoolean(true);
                Map<Pos, Collection<World>> poslookup = new Hashtable<>();
                worlds.forEach(w -> w.ownedRegions.forEach(p -> {
                    poslookup.computeIfAbsent(p, x -> new ArrayDeque<>()).add(w);
                }));
                logger.info("Loaded %d worlds with a total of %d different regions", worlds.size(), poslookup.size());

                totalRegions.set(poslookup.size());
                if (verbose.get()) {
                    new Thread(() -> {
                        while (running.get()) {
                            logger.info("Current progess: copying region %d/%d", currentRegion.get(), totalRegions.get());
                            try {
                                Thread.sleep(2000L);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }).start();
                }

                poslookup.entrySet().parallelStream().forEach((ThrowingConsumer<Map.Entry<Pos, Collection<World>>, IOException>) entry -> {
                    Pos pos = entry.getKey();
                    RegionFile outFile = outWorld.getRegionOrNull(pos);
                    CHECK:
                    if (!forceoverwrite.get() && outFile != null) {
                        for (int x = 31; x >= 0; x--) {
                            for (int z = 31; z >= 0; z--) {
                                if (!outFile.hasChunk(x, z)) {
                                    break CHECK;
                                }
                            }
                        }
                        outFile.close();
                        logger.trace("Skipping region (%d,%d) because it's already complete", pos.x, pos.z);
                        return;
                    }
                    Collection<World> potentialWorlds = entry.getValue();
                    Collection<RegionFile> regionFiles1 = inputorder.get() ?
                            potentialWorlds.stream()
                                           .map(world -> world.getRegionOrNull(pos))
                                           .filter(Objects::nonNull)
                                           .collect(Collectors.toCollection(ArrayDeque::new)) :
                            potentialWorlds.stream()
                                           .map(world -> world.getRegionOrNull(pos))
                                           .filter(Objects::nonNull)
                                           .sorted(Comparator.comparingLong(RegionFile::lastModified))
                                           .collect(Collectors.toCollection(ArrayDeque::new));
                    RUN:
                    {
                        LOOP:
                        for (RegionFile file : regionFiles1) {
                            for (int x = 31; x >= 0; x--) {
                                for (int z = 31; z >= 0; z--) {
                                    if (!file.hasChunk(x, z)) {
                                        continue LOOP;
                                    }
                                }
                            }
                            File f;
                            if (outFile == null) {
                                f = outWorld.getActualFileForRegion(pos);
                            } else {
                                outFile.close();
                                f = outFile.fileName;
                                outFile.fileName.delete();
                            }
                            file.close();
                            try (DataInputStream is = new DataInputStream(new FileInputStream(file.fileName));
                                 OutputStream os = new FileOutputStream(f)) {
                                byte[] buf = BUFFER_CACHE.get();
                                int filelen = (int) file.fileName.length();
                                is.readFully(buf, 0, filelen);
                                os.write(buf, 0, filelen);
                                totalSize.addAndGet(filelen);
                            }
                            break RUN;
                        }
                        if (outFile == null) {
                            outFile = outWorld.getOrCreateRegion(pos);
                        }
                        for (int x = 31; x >= 0; x--) {
                            for (int z = 31; z >= 0; z--) {
                                if (outFile.hasChunk(x, z)) {
                                    continue;
                                }
                                for (RegionFile file : regionFiles1) {
                                    if (file.hasChunk(x, z)) {
                                        try (DataInputStream is = new DataInputStream(file.getChunkDataInputStream(x, z));
                                             OutputStream os = outFile.getChunkDataOutputStream(x, z)) {
                                            byte[] buf = BUFFER_CACHE.get();
                                            int len;
                                            while ((len = is.read(buf)) != -1) {
                                                os.write(buf, 0, len);
                                                totalSize.addAndGet(len);
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (outFile != null) {
                        outFile.close();
                    }
                    regionFiles1.forEach((ThrowingConsumer<RegionFile, IOException>) RegionFile::close);
                    currentRegion.incrementAndGet();
                });
                running.set(false);
                time = System.currentTimeMillis() - time;
                logger.success(
                        "Added %d regions (%.2f MB) in %dh:%dm:%ds at %.2fMB/s (%.2f regions/s)",
                        totalRegions.get(),
                        (float) totalSize.get() / (1024.0f * 1024.0f),
                        time / (1000L * 60L * 60L),
                        time / (1000L * 60L) % 60,
                        time / (1000L) % 60,
                        ((float) totalSize.get() / (1024.0f * 1024.0f)) / (float) (time / 1000L),
                        (float) totalRegions.get() / (float) (time / 1000L)
                );
            }
            break;
            case "removeoutlying": {
                if (worlds.size() != 1) {
                    logger.error("Mode 'findmissing' requires exactly one input, but found %d", worlds.size());
                    return;
                }
                World world = ((ArrayDeque<World>) worlds).element();
                int removed = 0;
                long removedSize = 0L;

                Collection<Pos> positionsInRange = new HashSet<>();
                for (int x = findMinX.get(); x <= findMaxX.get(); x++) {
                    for (int z = findMinZ.get(); z <= findMaxZ.get(); z++) {
                        positionsInRange.add(new Pos(x, z));
                    }
                }
                for (File f : world.regions.listFiles()) {
                    String name = f.getName();
                    if (name.endsWith(".mca") && name.startsWith("r.")) {
                        String[] split = name.split("\\.");
                        int x = Integer.parseInt(split[1]);
                        int z = Integer.parseInt(split[2]);
                        if (!positionsInRange.contains(new Pos(x, z))) {
                            removedSize += f.length();
                            removed++;
                            f.deleteOnExit();
                        }
                    }
                }

                logger.success("Removed %d regions, totalling %.2f MB", removed, (float) removedSize / (1024.0f * 1024.0f));
            }
            break;
            default: {
                logger.error("Unknown mode: %s", mode);
                return;
            }
        }

        logger.success("Done!");
    }
}

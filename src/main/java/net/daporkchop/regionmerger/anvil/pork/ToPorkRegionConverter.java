package net.daporkchop.regionmerger.anvil.pork;

import lombok.NonNull;
import net.daporkchop.lib.primitive.map.ObjectLongMap;
import net.daporkchop.lib.primitive.map.hashmap.ObjectLongHashMap;
import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.ThrowingConsumer;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

/**
 * @author DaPorkchop_
 */
public class ToPorkRegionConverter {
    public static final File input = new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\Spawn_MojangAnvil");
    public static final File output_root = new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison");
    public static final World inWorld = new World(input);

    public static void main(String... args) {
        AtomicBoolean running = new AtomicBoolean(true);
        {
            Thread t = new Thread(() -> {
                try (Scanner scanner = new Scanner(System.in)) {
                    scanner.nextLine();
                }
                running.set(false);
            }, "keyboard interrupt listener");
            t.setDaemon(true);
            t.start();
        }
        long fullSize;
        {
            long l = 0L;
            for (Pos pos : inWorld.ownedRegions)    {
                l += inWorld.getActualFileForRegion(pos).length();
            }
            fullSize = l;
        }
        Map<String, Long> outputAmounts = new Hashtable<>();
        Map<String, Long> outputTimes = new Hashtable<>();
        PorkRegion.COMPRESSION_IDS.forEach((compressionId, helper) -> {
            if (!running.get()) {
                return;
            }
            System.out.printf("Converting input world to PorkAnvil v1 with compression: %s\n", helper);
            World outWorld;
            {
                File worldDir = new File(output_root, String.format("Spawn_PorkAnvil_v1_%s", helper.toString().replace(' ', '_')));
                rm(worldDir);
                outWorld = new World(worldDir, "pav1", true);
            }
            System.out.println("Copying non-region data...");
            Arrays.stream(inWorld.root.listFiles())
                    .filter(file -> !(file.getName().equals("region") || file.getName().startsWith("DIM")))
                    .forEach((ThrowingConsumer<File, IOException>) file -> copyFileRecursive(file, new File(outWorld.root, file.getName())));
            AtomicInteger convertedCounter = new AtomicInteger(0);
            AtomicLong outSize = new AtomicLong(0L);
            long time = System.currentTimeMillis();
            inWorld.ownedRegions.parallelStream()
            //inWorld.ownedRegions.stream()
                    .forEach((ThrowingConsumer<Pos, IOException>) regionPos -> {
                if (!running.get()) {
                    return;
                }
                System.out.printf("  Region %05d (out of %05d)\n", convertedCounter.getAndIncrement(), inWorld.ownedRegions.size());
                OverclockedRegionFile inFile = new OverclockedRegionFile(inWorld.getActualFileForRegion(regionPos));
                PorkRegion outFile = new PorkRegion(outWorld.getActualFileForRegion(regionPos));
                try {
                    for (int x = 31; x >= 0 && running.get(); x--) {
                        for (int z = 31; z >= 0 && running.get(); z--) {
                            if (outFile.hasChunk(x, z)) {
                                continue;
                            }
                            try (InputStream in = inFile.readData(x, z);
                                 OutputStream out = outFile.write(x, z, compressionId & 0xFF)) {
                                byte[] b = RegionMerger.BUFFER_CACHE.get();
                                int i;
                                while ((i = in.read(b)) != -1) {
                                    out.write(b, 0, i);
                                }
                            }
                        }
                    }
                    outSize.addAndGet(outFile.getSize());
                } finally {
                    inFile.close();
                    outFile.close();
                }
            });
            outputAmounts.put(helper.toString(), outSize.get());
            outputTimes.put(helper.toString(), System.currentTimeMillis() - time);
        });
        System.out.println("Done!");

        System.out.println();
        System.out.println("Sizes (lower is better):");
        System.out.printf("Original: %d bytes (%.3f GB)\n", fullSize, fullSize / (1024.0d * 1024.0d * 1024.0d));
        outputAmounts.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Long::compare))
                .forEachOrdered(e -> System.out.printf("  %d bytes (%.3f GB) for %s\n", e.getValue(), e.getValue() / (1024.0d * 1024.0d * 1024.0d), e.getKey()));

        System.out.println();
        System.out.println("Compression ratios (lower is better):");
        outputAmounts.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Long::compare))
                .forEachOrdered(e -> System.out.printf("  %.3f%% for %s\n", ((double) e.getValue() / (double) fullSize) * 100.0d, e.getKey()));

        System.out.println();
        System.out.println("Compression times (lower is better):");
        outputTimes.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Long::compare))
                .forEachOrdered(e -> System.out.printf("  %.02fh for %s\n", e.getValue() / (1000.0d * 60.0d * 60.0d), e.getKey()));
    }

    private static final void rm(@NonNull File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        if (!file.delete()) {
            throw new IllegalStateException(String.format("Could not delete %s: %s", file.isDirectory() ? "directory" : "file", file.getAbsolutePath()));
        }
    }

    private static final void copyFileRecursive(@NonNull File in, @NonNull File out) throws IOException {
        if (in.isDirectory()) {
            if (!out.mkdirs()) {
                throw new IllegalStateException(String.format("Could not create directory: %s", out.getAbsolutePath()));
            }
            for (File file : in.listFiles()) {
                copyFileRecursive(file, new File(out, file.getName()));
            }
        } else {
            if (!out.createNewFile()) {
                throw new IllegalStateException(String.format("Could not create file: %s", out.getAbsolutePath()));
            }
            try (InputStream is = new FileInputStream(in);
                 OutputStream os = new FileOutputStream(out)) {
                byte[] b = RegionMerger.BUFFER_CACHE.get();
                int i;
                while ((i = is.read(b)) != -1) {
                    os.write(b, 0, i);
                }
            }
        }
    }
}

package net.daporkchop.regionmerger.anvil.pork.tar;

import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileInputStream;
import lombok.NonNull;
import net.daporkchop.lib.binary.stream.DataIn;
import net.daporkchop.lib.binary.stream.DataOut;
import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.anvil.pork.ToPorkRegionConverter;
import net.daporkchop.regionmerger.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
public class ToTARConverter {
    //
    // Constants
    //
    private static final int REGIONS_PER_RUN = Integer.parseInt(System.getProperty("run.size", "16"));

    //
    // Other static things
    //
    private static final ThrowingBiConsumer<File, File, Exception> TAR_XZ_CREATOR;
    private static final ThrowingBiConsumer<Collection<File>, File, Exception> TAR_XZ_CREATOR_BULK;
    private static final ThrowingBiConsumer<File, File, Exception> TAR_XZ_EXTRACTOR;
    private static final File TMP_DIR = new File(System.getProperty("tmpDir", "./tmp/"));
    private static final File INPUT_DIR = new File(System.getProperty("input", "./in/"));
    private static final File OUTPUT_DIR = new File(System.getProperty("output", "./out/"));

    static {
        boolean force_native = System.getProperty("tar.forceNative", "false").equals("true");
        boolean force_java = System.getProperty("tar.forceJava", "false").equals("true");

        ThrowingBiConsumer<File, File, Exception> creator = null;
        ThrowingBiConsumer<Collection<File>, File, Exception> creator_bulk = null;
        ThrowingBiConsumer<File, File, Exception> extractor = null;
        try {
            if (force_java) {
                throw new Exception();
            }
            //if this throws an exception, then tar isn't on the local path
            Runtime.getRuntime().exec("tar --help").waitFor();

            creator = (in, out) -> {
                int exitCode = new ProcessBuilder()
                        .command("tar", "cfJ", String.format("%s.tar.xz", out.getAbsolutePath()), in.getName())
                        .directory(in.getParentFile())
                        .start()
                        .waitFor();
                if (exitCode != 0) {
                    throw new IllegalStateException(String.format("Compressor exit code: %d", exitCode));
                }
            };
            creator_bulk = (in, out) -> {
                ProcessBuilder processBuilder = new ProcessBuilder()
                        .command("tar", "cfJ", String.format("%s.tar.xz", out.getAbsolutePath()))
                        .directory(in.iterator().next().getParentFile());
                in.stream().map(File::getName).forEach(processBuilder.command()::add);
                int exitCode = processBuilder.start().waitFor();
                if (exitCode != 0) {
                    throw new IllegalStateException(String.format("Compressor exit code: %d", exitCode));
                }
            };
            extractor = (in, out) -> {
                if (out.exists()) {
                    if (!out.isDirectory()) {
                        throw new IllegalStateException(String.format("Not a directory: %s", out.getAbsolutePath()));
                    }
                } else if (!out.mkdirs()) {
                    throw new IllegalStateException(String.format("Couldn't create directory: %s", out.getAbsolutePath()));
                }
                int exitCode = new ProcessBuilder()
                        .command("tar", "xf", in.getAbsolutePath())
                        .directory(out)
                        .start()
                        .waitFor();
                //int exitCode = Runtime.getRuntime().exec(String.format("tar xf %s.tar.xz -C %s", in.getAbsolutePath(), out.getAbsolutePath())).waitFor();
                if (exitCode != 0) {
                    throw new IllegalStateException(String.format("Extractor exit code: %d", exitCode));
                }
            };
        } catch (Exception e) {
            System.out.printf("%s: native tar not found%s\n", force_native ? "Error" : "Warning", force_native ? "!" : ", using java implementation. This will lead to a serious decrease in performance!!!");
            if (force_native) {
                Runtime.getRuntime().exit(1);
            }
            System.out.println("Pausing for 10 seconds. If you wish to continue without native tar, please wait. Otherwise, please download and install something that adds tar to your system PATH (e.g. git-bash for Windows).");
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
            creator = (in, out) -> {
                //in theory we could implement it using TrueZip and Apache commons compress, but it'd be so slow it wouldn't be worth it at all
                throw new UnsupportedOperationException("Creating tar.xz archive without native tar");
            };
            creator_bulk = (in, out) -> {
                throw new UnsupportedOperationException("Creating tar.xz archive without native tar");
            };
            extractor = (in, out) -> {
                TFile archiveFile = new TFile(in.getAbsolutePath());
                if (out.exists() && !out.isDirectory()) {
                    throw new IllegalStateException(String.format("Not a directory: %s", out.getAbsolutePath()));
                }
                ___doExtractTarRecursive(archiveFile, out);
            };
        } finally {
            TAR_XZ_CREATOR = creator;
            TAR_XZ_CREATOR_BULK = creator_bulk;
            TAR_XZ_EXTRACTOR = extractor;
        }
        ToPorkRegionConverter.rm(TMP_DIR);
        if (!TMP_DIR.mkdirs()) {
            throw new IllegalStateException(String.format("Couldn't create directory: %s", TMP_DIR.getAbsolutePath()));
        }
    }

    private static void ___doExtractTarRecursive(@NonNull TFile in, @NonNull File out) throws IOException {
        TFile[] subFiles = in.listFiles();
        if (subFiles == null) {
            throw new IllegalStateException(String.format("Null file list in %s", in.getAbsolutePath()));
        }
        for (TFile archiveIn : subFiles) {
            File realOut = new File(out, archiveIn.getName());
            if (archiveIn.isDirectory()) {
                if (!realOut.mkdirs()) {
                    throw new IllegalStateException(String.format("Couldn't create directory: %s", realOut.getAbsolutePath()));
                }
                ___doExtractTarRecursive(archiveIn, realOut);
            } else {
                if (!realOut.createNewFile()) {
                    throw new IllegalStateException(String.format("Couldn't create file: %s", realOut.getAbsolutePath()));
                }
                try (InputStream is = new TFileInputStream(archiveIn);
                     OutputStream os = new FileOutputStream(realOut)) {
                    byte[] b = RegionMerger.BUFFER_CACHE.get();
                    int i;
                    while ((i = is.read(b)) != -1) {
                        os.write(b, 0, i);
                    }
                }
            }
        }
    }

    public static void main(String... args) {
        String mode = "";
        for (String s : args) {
            mode = s;
        }
        switch (mode) {
            case "":
            case "-h":
            case "--h":
            case "-help":
            case "--help":
            case "h":
            case "help": {
                System.out.println("PorkWorldArchiver v0.0.1");
                System.out.println("Copyright (c) 2018 DaPorkchop_");
                System.out.println("https://daporkchop.net");
                System.out.println();
                System.out.println("Modes:");
                System.out.println("  a    Archives a world");
                System.out.println("  x    Extracts a world");
                System.out.println();
                System.out.println("Options: (these go before the '-jar' part of the command)");
                System.out.println("  -Dtar.forceNative=<bool>         Whether or not to force usage of native tar implementation. Default=false");
                System.out.println("  -Dtar.forceJava=<bool>           Whether or not to force usage of java-based tar implementation. Default=false");
                System.out.println("  -Dinput=<path>                   Give the path to the input directory. Default=./in/");
                System.out.println("  -Doutput=<path>                  Give the path to the output directory. Default=./out/");
                System.out.println("  -DtmpDir=<path>                  Give the path to the temporary file directory. Default=./tmp/");
                System.out.println("  -Drun.size=<size>                The number of regions per run. Smaller values are faster but make compression less effective. Default=16");
            }
            return;
            case "a": {
                if ((!OUTPUT_DIR.exists() && !OUTPUT_DIR.mkdirs()) || !OUTPUT_DIR.isDirectory()) {
                    throw new IllegalStateException(String.format("Couldn't create directory: %s", OUTPUT_DIR.getAbsolutePath()));
                }
                System.out.println("Archiving extra data...");
                TAR_XZ_CREATOR_BULK.accept(
                        Arrays.stream(INPUT_DIR.listFiles())
                                .filter(f -> {
                                    String name = f.getName();
                                    return !(name.startsWith("DIM") || name.equals("region"));
                                })
                                .collect(Collectors.toCollection(ArrayDeque::new)),
                        new File(OUTPUT_DIR, "extra")
                );
                System.out.printf("Preparing to archive world %s...\n", INPUT_DIR.getAbsolutePath());
                World inWorld = new World(INPUT_DIR);
                List<Collection<Pos>> runs = new ArrayList<>();
                {
                    //create a bunch of runs of regions
                    AtomicInteger i = new AtomicInteger(0);
                    inWorld.ownedRegions.stream()
                            .sorted(Comparator.comparingLong(pos -> ((long) -pos.x * Integer.MAX_VALUE) - pos.z))
                            .forEach(pos -> {
                                int j = i.getAndIncrement();
                                int k = j % REGIONS_PER_RUN;
                                if (k == 0) {
                                    runs.add(new ArrayDeque<>());
                                }
                                runs.get(j / REGIONS_PER_RUN).add(pos);
                            });
                }
                AtomicLong runCounter = new AtomicLong(0L);
                (false ? runs.stream() : runs.parallelStream()).forEach(fullRun -> {
                    byte[] b = RegionMerger.BUFFER_CACHE.get();
                    long runId = runCounter.getAndIncrement();
                    File runDir = new File(TMP_DIR, String.format("run_%05d", runId));
                    System.out.printf("Extracting %d regions for run %d...\n", fullRun.size(), runId);
                    fullRun.forEach(regionPos -> {
                        File theRegionFile = inWorld.getActualFileForRegion(regionPos);
                        File outDir = new File(runDir, String.format("r.%d.%d", regionPos.x, regionPos.z));
                        int counter = 0;
                        try (OverclockedRegionFile regionFile = new OverclockedRegionFile(theRegionFile)) {
                            for (int x = 31; x >= 0; x--) {
                                for (int z = 31; z >= 0; z--) {
                                    if (!regionFile.hasChunk(x, z)) {
                                        continue;
                                    }
                                    if (!outDir.exists() && !outDir.mkdirs()) {
                                        throw new IllegalStateException(String.format("Couldn't create directory: %s", outDir.getAbsolutePath()));
                                    }
                                    File chunkFile = new File(outDir, String.valueOf(counter++));
                                    if (!chunkFile.createNewFile()) {
                                        throw new IllegalStateException(String.format("Couldn't create file: %s", chunkFile.getAbsolutePath()));
                                    }
                                    try (InputStream is = regionFile.readData(x, z);
                                         DataOut os = DataOut.wrap(new ThreadLocalBufferedOutputStream(new FileOutputStream(chunkFile)))) {
                                        os.write(x);
                                        os.write(z);
                                        //TODO: more efficient encoding format
                                        int i;
                                        while ((i = is.read(b)) != -1) {
                                            os.write(b, 0, i);
                                        }
                                    }
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    System.out.printf("Compressing %d regions for run %d...\n", fullRun.size(), runId);
                    TAR_XZ_CREATOR.accept(runDir, new File(OUTPUT_DIR, String.format("run_%05d", runId)));
                    ToPorkRegionConverter.rm(runDir);
                    System.out.printf("Completed run %d!\n", runId);
                });
            }
            break;
            case "x": {
                //TAR_XZ_EXTRACTOR.accept(INPUT_DIR, OUTPUT_DIR);
                if (OUTPUT_DIR.exists()) {
                    ToPorkRegionConverter.rm(OUTPUT_DIR);
                }
                if (!OUTPUT_DIR.mkdirs()) {
                    throw new IllegalStateException(String.format("Couldn't create directory: %s", OUTPUT_DIR.getAbsolutePath()));
                } else if (!new File(OUTPUT_DIR, "region").mkdir()) {
                    throw new IllegalStateException("Couldn't create region directory!");
                }
                System.out.println("Extracting extra data...");
                TAR_XZ_EXTRACTOR.accept(new File(INPUT_DIR, "extra.tar.xz"), OUTPUT_DIR);
                System.out.printf("Preparing to extract world %s...\n", INPUT_DIR.getAbsolutePath());
                Arrays.stream(INPUT_DIR.listFiles())
                        .filter(file -> file.getName().startsWith("run_") && file.getName().endsWith(".tar.xz"))
                        .parallel()
                        .forEach((ThrowingConsumer<File, IOException>) archive -> {
                            byte[] b = RegionMerger.BUFFER_CACHE.get();
                            System.out.printf("Extracting run %s...\n", archive.getAbsolutePath());
                            File outDir = new File(TMP_DIR, archive.getName().replace(".tar.xz", ""));
                            TAR_XZ_EXTRACTOR.accept(archive, TMP_DIR);
                            for (File regionDir : outDir.listFiles()) {
                                int x;
                                int z;
                                {
                                    String[] split = regionDir.getName().split("\\.");
                                    x = Integer.parseInt(split[1]);
                                    z = Integer.parseInt(split[2]);
                                }
                                try (OverclockedRegionFile regionFile = new OverclockedRegionFile(new File(OUTPUT_DIR, String.format("region/r.%d.%d.mca", x, z)))) {
                                    for (File chunkFile : regionDir.listFiles()) {
                                        try (DataIn in = DataIn.wrap(new CachedFileInput(chunkFile))) {
                                            int chunkX = in.read();
                                            int chunkZ = in.read();
                                            try (OutputStream out = regionFile.write(chunkX, chunkZ, 2)) {
                                                int i;
                                                while ((i = in.read(b)) != -1) {
                                                    out.write(b, 0, i);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            ToPorkRegionConverter.rm(outDir);
                        });
            }
            break;
            default: {
                System.out.printf("Unknown mode: %s\n", mode);
            }
            return;
        }
        System.out.println("Done!");
    }
}

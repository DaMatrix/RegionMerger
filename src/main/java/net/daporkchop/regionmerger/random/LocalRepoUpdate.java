package net.daporkchop.regionmerger.random;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.NonNull;
import net.daporkchop.lib.binary.UTF8;
import net.daporkchop.regionmerger.RegionMerger;
import net.daporkchop.regionmerger.util.ThrowingConsumer;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.*;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.updater.*;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LocalRepoUpdate {
    private static RepositorySystem newRepositorySystem(DefaultServiceLocator locator) {
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        return locator.getService(RepositorySystem.class);
    }

    private static RepositorySystemSession newSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        LocalRepository localRepo = new LocalRepository("../repository/");
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
        return session;
    }

    public static void main(String... args) throws Exception {
        List<RemoteRepository> repositories = Arrays.stream(args)
                .map(s -> new RemoteRepository.Builder(null, "default", s).build())
                .collect(Collectors.toList());
        Set<String> missing;
        if (true) {
            PlexusContainer plexusContainer;
            IndexUpdater updater;
            Wagon httpWagon;
            Indexer indexer;
            {
                final DefaultContainerConfiguration config = new DefaultContainerConfiguration();
                config.setClassPathScanning(PlexusConstants.SCANNING_INDEX);
                plexusContainer = new DefaultPlexusContainer(config);
                indexer = plexusContainer.lookup(Indexer.class);
                updater = plexusContainer.lookup(IndexUpdater.class);
                httpWagon = plexusContainer.lookup(Wagon.class, "http");
            }
            if (false) {
                indexer = new DefaultIndexer(
                        new DefaultSearchEngine(),
                        new DefaultIndexerEngine(),
                        new DefaultQueryCreator()
                );
            }
            IndexingContext context = indexer.createIndexingContext(
                    "repo-index",
                    "repo-index",
                    new File("../repository"),
                    new File("../repository/.index/repo-index"),
                    "https://maven.daporkchop.net/",
                    null,
                    true,
                    true,
                    Arrays.asList(
                            plexusContainer.lookup(IndexCreator.class, "min"),
                            plexusContainer.lookup(IndexCreator.class, "jarContent"),
                            plexusContainer.lookup(IndexCreator.class, "maven-plugin")
                    )
            );
            {
                System.out.println("Updating Index...");
                System.out.println("This might take a while on first run, so please be patient!");
                // Create ResourceFetcher implementation to be used with IndexUpdateRequest
                // Here, we use Wagon based one as shorthand, but all we need is a ResourceFetcher implementation
                TransferListener listener = new AbstractTransferListener() {
                    public void transferStarted(TransferEvent transferEvent) {
                        System.out.print("  Downloading " + transferEvent.getResource().getName());
                    }

                    public void transferProgress(TransferEvent transferEvent, byte[] buffer, int length) {
                    }

                    public void transferCompleted(TransferEvent transferEvent) {
                        System.out.println(" - Done");
                    }
                };
                ResourceFetcher resourceFetcher = new WagonHelper.WagonFetcher(httpWagon, listener, null, null);

                Date centralContextCurrentTimestamp = context.getTimestamp();
                IndexUpdateRequest updateRequest = new IndexUpdateRequest(context, resourceFetcher);
                IndexUpdateResult updateResult = updater.fetchAndUpdateIndex(updateRequest);
                if (updateResult.isFullUpdate()) {
                    System.out.println("Full update happened!");
                    //} else if (updateResult.getTimestamp().equals(centralContextCurrentTimestamp)) {
                    //    System.out.println("No update needed, index is up to date!");
                } else {
                    System.out.println(
                            "Incremental update happened, change covered " + centralContextCurrentTimestamp + " - "
                                    + updateResult.getTimestamp() + " period.");
                }

                System.out.println();
            }
            {
                Map<String, String> contents = new Hashtable<>();
                IndexSearcher searcher = context.acquireIndexSearcher();
                try {
                    IndexReader reader = searcher.getIndexReader();
                    Bits liveDocs = MultiFields.getLiveDocs(reader);
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        if (liveDocs == null || liveDocs.get(i)) {
                            Document document = reader.document(i);
                            ArtifactInfo info = IndexUtils.constructArtifactInfo(document, context);
                            if (info == null) {
                                continue;
                            }
                            //String identifier = String.format("%s:%s:%s", info.getGroupId(), info.getArtifactId(), info.getVersion());
                            String identifier = String.format("%s:%s:jar:sources:%s", info.getGroupId(), info.getArtifactId(), info.getVersion());
                            if (info.getClassifier() == null) {
                                contents.putIfAbsent(identifier, "");
                            } else {
                                contents.put(identifier, info.getClassifier());
                            }
                        }
                    }
                } finally {
                    context.releaseIndexSearcher(searcher);
                }
                //File file = new File("/home/daporkchop/.m2/missingsources.txt");
                //if (file.exists() && !file.isFile()) {
                //    throw new IllegalStateException(String.format("not a file: %s", file.getAbsolutePath()));
                //}
                //try (OutputStream out = new FileOutputStream(file)) {
                //    contents.entrySet().stream()
                //            .filter(e -> e.getValue().isEmpty())
                //            .map(Map.Entry::getKey)
                //.filter(s -> !s.contains("maven"))
                //.filter(s -> !s.contains("plugin"))
                //.filter(s -> !s.contains("plexus"))
                //.map(s -> String.format("%s:sources\n", s).getBytes(UTF8.utf8))
                //            .map(s -> String.format("%s\n", s).getBytes(UTF8.utf8))
                //            .forEach((ThrowingConsumer<byte[], IOException>) out::write);
                //}
                missing = contents.entrySet().parallelStream()
                        .filter(e -> e.getValue().isEmpty())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
            }
        }
        if (true) {
            File file = new File(".", "missing.txt");
            if (file.exists()) {
                List<String> list;
                try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file))) {
                    list = IOUtils.readLines(reader);
                }
                list.stream().distinct().filter(s -> !s.isEmpty()).forEach(missing::remove);
            }

            DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
            RepositorySystem system = newRepositorySystem(locator);
            RepositorySystemSession session = newSession(system);

            AtomicInteger total = new AtomicInteger(0);
            AtomicInteger good = new AtomicInteger(0);
            AtomicInteger bad = new AtomicInteger(0);
            Set<String> downloaded = Collections.synchronizedSet(new HashSet<>());
            missing.parallelStream()
                    .distinct()
                    .filter(s -> !s.isEmpty())
                    .forEach(id -> {
                        total.incrementAndGet();
                        System.out.printf("Resolving %s...\n", id);
                        try {
                            system.resolveArtifact(session, new ArtifactRequest(new DefaultArtifact(id), repositories, null));
                            System.out.printf("%s downloaded successfully!\n", id);
                            good.incrementAndGet();
                            downloaded.add(id);
                        } catch (ArtifactResolutionException e) {
                            System.out.printf("Unable to find %s!\n", id);
                            bad.incrementAndGet();
                        }
                    });
            System.out.printf("Total: %d, resolved: %d, failed: %d\n", total.get(), good.get(), bad.get());
            downloaded.stream().distinct().forEach(missing::remove);
            System.out.println("Saving list of unresolvable artifacts...");
            if (!file.exists() && !file.createNewFile()) {
                throw new IllegalStateException(String.format("Couldn't create file: %s", file.getAbsolutePath()));
            }
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file, true))) {
                missing.stream()
                        .map(s -> String.format("%s\n\n", s).getBytes(UTF8.utf8))
                        .forEach((ThrowingConsumer<byte[], IOException>) os::write);
            }
            System.out.println("Done!");
        }
        System.out.println("Generating hashes....");
        {
            Set<File> toHash = Collections.synchronizedSet(new HashSet<>());
            //findHashableFiles(new File("/home/daporkchop/.m2/repository/.index/repo-index"), toHash);
            findHashableFiles(new File("../repository"), toHash);
            System.out.printf("Need to hash %d files.\n", toHash.size());
            toHash.parallelStream().forEach((ThrowingConsumer<File, IOException>) file -> {
                System.out.printf("Hashing %s...\n", file.getAbsolutePath());
                File md5 = new File(String.format("%s.md5", file.getAbsolutePath()));
                if (!md5.exists() && !md5.createNewFile()) {
                    throw new IllegalStateException(String.format("Unable to create file: %s", md5.getAbsolutePath()));
                }
                File sha1 = new File(String.format("%s.sha1", file.getAbsolutePath()));
                if (!sha1.exists() && !sha1.createNewFile()) {
                    throw new IllegalStateException(String.format("Unable to create file: %s", sha1.getAbsolutePath()));
                }
                try (OutputStream md5os = new FileOutputStream(md5);
                     OutputStream sha1os = new FileOutputStream(sha1);
                     InputStream in = new FileInputStream(file)) {
                    byte[] buf = RegionMerger.BUFFER_CACHE.get();
                    Hasher md5hasher = Hashing.md5().newHasher();
                    Hasher sha1hasher = Hashing.sha1().newHasher();
                    int i;
                    while ((i = in.read(buf)) != -1) {
                        md5hasher.putBytes(buf, 0, i);
                        sha1hasher.putBytes(buf, 0, i);
                    }
                    md5os.write(md5hasher.hash().toString().getBytes(UTF8.utf8));
                    sha1os.write(sha1hasher.hash().toString().getBytes(UTF8.utf8));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void findHashableFiles(@NonNull File file, @NonNull Set<File> toHash) {
        if (file.isDirectory()) {
            Set<File> contents = Arrays.stream(file.listFiles()).filter(File::isFile).collect(Collectors.toSet());
            contents.stream()
                    .filter(f -> { //eliminate hash files
                        String s = f.getName();
                        return !s.endsWith(".md5") && !s.endsWith(".sha1");
                    })
                    .filter(f -> { //find files without hash
                        File md5 = new File(String.format("%s.md5", f.getAbsolutePath()));
                        File sha1 = new File(String.format("%s.sha1", f.getAbsolutePath()));
                        return !md5.exists() || !sha1.exists();
                    })
                    .forEach(toHash::add);
            Arrays.stream(file.listFiles())
                    .filter(File::isDirectory)
                    .filter(f -> !f.getName().equals(".index"))
                    .forEach(f -> findHashableFiles(f, toHash));
        } else {
            throw new IllegalStateException();
        }
    }
}

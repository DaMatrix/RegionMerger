import net.daporkchop.lib.binary.UTF8;
import net.daporkchop.regionmerger.util.ThrowingConsumer;
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
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class LocalRepoUpdate {
    @Test
    public void test() throws Exception {
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
                "local",
                "local",
                new File("/home/daporkchop/.m2/repository"),
                new File("/home/daporkchop/.m2/index"),
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
                        String identifier = String.format("%s:%s:%s", info.getGroupId(), info.getArtifactId(), info.getVersion());
                        if (info.getClassifier() == null)   {
                            contents.putIfAbsent(identifier, "");
                        } else {
                            contents.put(identifier, info.getClassifier());
                        }
                    }
                }
            } finally {
                context.releaseIndexSearcher(searcher);
            }
            File file = new File("/home/daporkchop/.m2/missingsources.txt");
            if (file.exists() && !file.isFile())  {
                throw new IllegalStateException(String.format("not a file: %s", file.getAbsolutePath()));
            }
            try (OutputStream out = new FileOutputStream(file)) {
                contents.entrySet().stream()
                        .filter(e -> e.getValue().isEmpty())
                        .map(Map.Entry::getKey)
                        .filter(s -> !s.contains("maven"))
                        .filter(s -> !s.contains("plugin"))
                        .filter(s -> !s.contains("plexus"))
                        .map(s -> String.format("%s:sources\n", s).getBytes(UTF8.utf8))
                        .forEach((ThrowingConsumer<byte[], IOException>) out::write);
            }
        }
    }
}

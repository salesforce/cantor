package com.salesforce.cantor.archive.s3;

import com.adobe.testing.s3mock.testng.S3Mock;
import com.adobe.testing.s3mock.testng.S3MockListener;
import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.EventsChunk;
import com.salesforce.cantor.archive.TestUtils;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.misc.archivable.impl.ArchivableCantor;
import com.salesforce.cantor.s3.CantorOnS3;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Listeners(value = { S3MockListener.class })
public class ArchiverOnS3Test {
    private static final long timeframeBound = System.currentTimeMillis();
    private static final long timeframeOrigin = timeframeBound - TimeUnit.DAYS.toMillis(2);
    private static final String h2Directory = "/tmp/cantor-local-test";
    private static final String archivePathBase = "cantor-s3-archive-data";
    private static final long hourMillis = TimeUnit.HOURS.toMillis(1);
    private static final String archiveNamespace = "events-archive";

    private Map<String, Long> cantorH2Namespaces;
    private Cantor cantorLocal;
    private CantorOnS3 cantorOnS3;
    private ArchiverOnS3 archiver;

    @BeforeMethod
    public void setUp() throws IOException {
        final AmazonS3 s3Client = createS3Client();
        this.cantorOnS3 = new CantorOnS3(s3Client, "cantor-archive-test");
        this.archiver = new ArchiverOnS3(cantorOnS3);

        this.cantorLocal = new ArchivableCantor(new CantorOnH2(h2Directory), this.archiver);
        this.cantorH2Namespaces = new HashMap<>();
        TestUtils.generateData(this.cantorLocal, timeframeOrigin, timeframeBound, cantorH2Namespaces);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        // delete test cantor data
        for (final String cantorH2Namespace : this.cantorH2Namespaces.keySet()) {
            this.cantorLocal.events().drop(cantorH2Namespace);
        }

        for (final String namespace : this.cantorOnS3.objects().namespaces()) {
            this.cantorOnS3.objects().drop(namespace);
        }
        createS3Client().deleteBucket(String.format("%s-all-namespaces", "cantor-archive-test"));

        // delete test archive
        final File baseDirectory = new File("cantor-s3-archive-data");
        final String[] entries = baseDirectory.list();
        if (entries != null) {
            for(final String file : entries) {
                final File currentFile = new File(baseDirectory.getPath(), file);
                currentFile.delete();
            }
        }
        baseDirectory.delete();
    }

    /**
     * Recommended for testing with real s3 clients as up to 1 GB per namespace can be transferred
     */
    @Test
    public void testEventsArchiveSingleNamespace() throws IOException {
        final Map.Entry<String, Long> cantorH2Namespace = cantorH2Namespaces.entrySet().iterator().next();
        final List<Events.Event> totalEvents = this.cantorLocal.events()
                .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);

        final List<Events.Event> events = this.cantorLocal.events()
                .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
        this.cantorLocal.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());

        // check that at least one archive was made (which should always be true since we generate events at timestamp 0)
        final Collection<String> archiveFilenames = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
        final List<String> matchingArchives = ((EventsArchiverOnS3) this.archiver.events())
                .getMatchingArchives(cantorH2Namespace.getKey(), archiveFilenames, 0, cantorH2Namespace.getValue());
        Assert.assertNotEquals(matchingArchives.size(), 0);

        // restore the events
        final List<Events.Event> restoreEvents = this.cantorLocal.events()
                .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
        Assert.assertEquals(restoreEvents.size(), events.size(), getTestInfo(cantorH2Namespace, "all events were not restored"));

        // sanity check no events have been lost
        final List<Events.Event> totalEventsAgain = this.cantorLocal.events()
                .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
        Assert.assertEquals(totalEventsAgain.size(), totalEvents.size(), getTestInfo(cantorH2Namespace, "more events were expired than were archived"));
    }

    @Test
    public void testEventsArchive() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : this.cantorH2Namespaces.entrySet()) {
            final List<Events.Event> totalEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);

            final List<Events.Event> events = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            this.cantorLocal.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());

            // check that at least one archive was made (which should always be true since we generate events at timestamp 0)
            final Collection<String> archiveFilenames = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
            final List<String> matchingArchives = ((EventsArchiverOnS3) this.archiver.events())
                    .getMatchingArchives(cantorH2Namespace.getKey(), archiveFilenames, 0, cantorH2Namespace.getValue());
            Assert.assertNotEquals(matchingArchives.size(), 0, getTestInfo(cantorH2Namespace, "no archives were created"));

            // restore the events
            final List<Events.Event> restoreEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            Assert.assertEquals(restoreEvents.size(), events.size(), getTestInfo(cantorH2Namespace, "all events were not restored"));

            // sanity check no events have been lost
            final List<Events.Event> totalEventsAgain = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(totalEventsAgain.size(), totalEvents.size(), getTestInfo(cantorH2Namespace, "more events were expired than were archived"));
        }
    }

    @Test
    public void testEventsArchiveIdempotent() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : this.cantorH2Namespaces.entrySet()) {
            final List<Events.Event> totalEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);

            final List<Events.Event> events = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            this.cantorLocal.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            validateArchive(events, cantorH2Namespace.getKey(), cantorH2Namespace.getValue());

            // run again; restoring events
            final List<Events.Event> sameEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, cantorH2Namespace.getValue());
            Assert.assertEquals(sameEvents.size(), events.size(), getTestInfo(cantorH2Namespace, "all events were not restored"));
            this.cantorLocal.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            // intentionally checking that sameEvents had no impact by validating with events
            validateArchive(events, cantorH2Namespace.getKey(), cantorH2Namespace.getValue());

            // run again restoring all events then expire again with one new event;
            this.cantorLocal.events().store(cantorH2Namespace.getKey(), timeframeOrigin, null, null);
            final List<Events.Event> allEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            final List<Events.Event> allEventsAgain = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(allEvents.size(), allEventsAgain.size(), getTestInfo(cantorH2Namespace, "incorrect number of events after second call"));
            this.cantorLocal.events().expire(cantorH2Namespace.getKey(), timeframeBound);
            validateArchive(allEventsAgain, cantorH2Namespace.getKey(), timeframeBound);

            // last run with dirtied archive file
            final List<Events.Event> refreshedEvents = this.cantorLocal.events()
                    .get(cantorH2Namespace.getKey(), timeframeOrigin, timeframeBound);
            Assert.assertEquals(refreshedEvents.size(), allEvents.size(), getTestInfo(cantorH2Namespace, "incorrect number of events after restoration"));

            // sanity check no events have been lost
            // plus one for the extra event we added mid test
            Assert.assertEquals(refreshedEvents.size(), totalEvents.size() + 1, getTestInfo(cantorH2Namespace, "more events were expired than were archived"));
        }
    }

    private void validateArchive(final List<Events.Event> events,
                                 final String namespace,
                                 final long endTimestamp) throws IOException {
        if (events.size() > 0) {
            final Collection<String> keys = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
            final EventsArchiverOnS3 eventsArchiver = ((EventsArchiverOnS3) this.archiver.events());
            final List<String> fileArchive = eventsArchiver.getMatchingArchives(namespace, keys, timeframeOrigin, endTimestamp);

            int eventCount = 0;
            for (final String file : fileArchive) {
                final Path archiveLocation = Paths.get(archivePathBase, file);
                eventsArchiver.pullFile(file, archiveLocation);
                try (final ArchiveInputStream archiveInputStream = getArchiveInputStream(archiveLocation)) {
                    while (archiveInputStream.getNextEntry() != null) {
                        final EventsChunk chunk = EventsChunk.parseFrom(archiveInputStream);
                        eventCount += chunk.getEventsList().stream()
                                .filter(event -> event.getTimestampMillis() >= timeframeOrigin
                                && event.getTimestampMillis() <= endTimestamp).count();
                    }
                }
                archiveLocation.toFile().delete();
            }
            Assert.assertEquals(eventCount, events.size(), "events that were expired were not archived");
        } else {
            final String[] files = new File(archivePathBase).list();
            if (files == null) return;

            final List<String> filesList = Arrays.asList(files);
            filesList.forEach(file -> Assert.assertFalse(file.contains(namespace), "no events archived but found archive file for namespace: " + namespace));
        }
    }


    private ArchiveInputStream getArchiveInputStream(final Path archiveFile) throws IOException {
        return new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(archiveFile))));
    }

    private String getTestInfo(final Map.Entry<String, Long> cantorH2Namespace, final String message) {
        // [namespace][origin-expiry-bound]msg
        return String.format("[%s][%d-%d-%d] %s",
                cantorH2Namespace.getKey(),
                timeframeOrigin,
                cantorH2Namespace.getValue(),
                timeframeBound,
                message);
    }

    // insert real S3 client here to run integration testing
    private AmazonS3 createS3Client() {
        return S3Mock.getInstance().createS3Client("us-west-1");
    }
}

package com.salesforce.cantor.s3;

import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.io.ByteStreams;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.common.AbstractBaseEventsTest;
import com.salesforce.cantor.grpc.CantorOnGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@Test(enabled = false)
public class EventsOnS3Test extends AbstractBaseEventsTest {
    private static final String credentialsLocation = "/tmp/secrets";

    @Override
    protected Cantor getCantor() throws IOException {
        final AmazonS3 s3Client = createS3Client();
        return new CantorOnS3(s3Client, "warden-cantor--monitoring--dev1--us-west-2--dev");
    }

    // insert real S3 client here to run integration testing
    private static AmazonS3 createS3Client() throws IOException {
        final File s3File = new File(credentialsLocation);
        try (final Scanner csvReader = new Scanner(new FileInputStream(s3File))) {
            csvReader.useDelimiter(",");

            final String keyId = csvReader.next();
            final String accessKey = csvReader.next();

            final AWSCredentials sessionCredentials = new BasicAWSCredentials(keyId, accessKey);
            return AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2)
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .build();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger("foo");
    public static void main(final String[] args) throws IOException, InterruptedException {

        final AmazonS3 s3Client = createS3Client();
        final String bucketName = "warden-cantor--monitoring--dev1--us-west-2--dev";
        final Cantor cantor = new CantorOnS3(s3Client, bucketName);




        final Map<String, String> metadata = new HashMap<>();
        final Map<String, Double> dimensions = new HashMap<>();
        final String uuid = UUID.randomUUID().toString();
        metadata.put("key", "value");
        metadata.put("foo", uuid);
        metadata.put("bar", uuid);
        dimensions.put("key", 7.0);
        dimensions.put("foo", ThreadLocalRandom.current().nextDouble());
        dimensions.put("bar", ThreadLocalRandom.current().nextDouble());
        final String namespacePrefix = "stress-testing-";

        for (int n = 0; n < 10; ++n) {
            final String namespace = namespacePrefix + "-" + n;
//            cantor.events().create(namespace);
        }

//        final long before = System.nanoTime();
//        for (int i = 0; i < 1_000_000; ++i) {
//            final String namespace = namespacePrefix + "-" + (i % 10);
//            cantor.events().store(namespace, ThreadLocalRandom.current().nextLong(System.currentTimeMillis() - 1000 * 60 * 10, System.currentTimeMillis()), metadata, dimensions, uuid.getBytes());
//            Thread.sleep(1);
//            logger.info("events: {}", cantor.events().get(namespace, 0, Long.MAX_VALUE));
//        }
//        logger.info("time spent: {}ms", (System.nanoTime() - before) / 1000000);


        final Events.Event t = new Events.Event(0, null, null, uuid.getBytes());
        for (int i = 0; i < 10; ++i) {
            cantor.events().store("maiev-tenant-autobuild-prd", t);
        }

        logger.info("returned: {}", cantor.events().get("maiev-tenant-autobuild-prd", 0, 0, null, null, true));


//        final byte[] bytes = S3Utils.getObjectBytes(s3Client, bucketName, "maiev-tenant-falcon-aws-prod1-useast1", 98, 98 + 48 - 1);
//        logger.info("48 bytes after 48th byte are: '{}'", new String(bytes));
//        Thread.sleep(1000000);

        final Map<String, String> query = new HashMap<>();
        query.put("changeList", "=29235163");
//        query.put("name", "=top");
        final List<Events.Event> events = cantor.events().get("maiev-tenant-autobuild-prd", 1605776701237L - TimeUnit.HOURS.toMillis(10), 1605776701237L + TimeUnit.HOURS.toMillis(10), query, null, true);
        for (final Events.Event event : events) {
            logger.info("event name is: {}", event.getMetadata().get("name"));
            logger.info("event payload is: {}", new String(decompress(event.getPayload())));
        }
//        logger.info("events found: {}", cantor.events().get("maiev-tenant-autobuild-prd", 1605776701237L - TimeUnit.HOURS.toMillis(10), 1605776701237L + TimeUnit.HOURS.toMillis(10), query, null, true));
        Thread.sleep(1000000);


//        final String namespacePrefix = "testing";
        logger.info("namespaces: {}", cantor.events().namespaces());
        final ExecutorService executor = Executors.newCachedThreadPool();
        executor.submit(() -> {
            try {
                run(cantor, "maiev-tenant-falcon-aws-prod1-useast1");
            } catch (IOException e) {
                logger.warn("failed", e);
            }
        });
        executor.submit(() -> {
            try {
                run(cantor, "maiev-tenant-falcon-aws-stage1-useast2");
            } catch (IOException e) {
                logger.warn("failed", e);
            }
        });
        executor.submit(() -> {
            try {
                run(cantor, "maiev-tenant-falcon-aws-prod2-apsouth1");
            } catch (IOException e) {
                logger.warn("failed", e);
            }
        });
        executor.submit(() -> {
            try {
                run(cantor, "maiev-tenant-autobuild-prd");
            } catch (IOException e) {
                logger.warn("failed", e);
            }
        });

        executor.awaitTermination(1, TimeUnit.HOURS);

//        Thread.sleep(1000000);


    }

    public static byte[] decompress(final byte[] compressedBytes) throws IOException {
        final byte[] unzippedPayload;
        try (final ByteArrayInputStream byteStream = new ByteArrayInputStream(compressedBytes);
             final GZIPInputStream gzis = new GZIPInputStream(byteStream)) {
            unzippedPayload = ByteStreams.toByteArray(gzis);
        }

        return unzippedPayload;
    }
    private static void run(Cantor cantor, final String n) throws IOException {
        final Cantor prdCantor = new CantorOnGrpc("cantor.casp.prd-samtwo.prd.slb.sfdc.net:11983");
        cantor.events().create(n);
        for (long end = System.currentTimeMillis(); end > System.currentTimeMillis() - TimeUnit.DAYS.toMillis(6); end -= TimeUnit.MINUTES.toMillis(10)) {
            boolean retry;
            do {
                try {
                    logger.info("fetching events...");
                    final Collection<Events.Event> events = prdCantor.events().get(n, end - TimeUnit.MINUTES.toMillis(10), end, true);
                    logger.info("storing {} events", events.size());
                    cantor.events().store(n, events);
                    logger.info("successfully stored them all.");
                    retry = false;
                } catch (Exception e) {
                    logger.warn("exception - retrying...");
                    retry = true;
                }
            } while (retry);
        }
        logger.info("all done!");
    }

    @Test
    public void testStoreOneEvent() throws Exception {
        // no-op as this test creates a payload too large for s3 select
    }

    @Override
    public void testMetadata() throws Exception {
        // no-op as it's not implement yet
    }

    @Override
    public void testAggregations() throws Exception {
        // no-op as it's not implement yet
    }
}
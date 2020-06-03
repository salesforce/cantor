package com.salesforce.cantor.archive.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.archive.file.FileArchiver;
import com.salesforce.cantor.grpc.CantorOnGrpc;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.s3.StreamingObjects;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class S3ObjectsArchiverTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String CANTOR_S3_NAMESPACE = "cantor-archive-test";

    private static final Map<String, Long> CANTOR_H2_NAMESPACES = new HashMap<>();
    private static final long TIMEFRAME_BOUND = System.currentTimeMillis();
    private static final long TIMEFRAME_ORIGIN = TIMEFRAME_BOUND - TimeUnit.DAYS.toMillis(2);
    private static final String H2_DIRECTORY = "/tmp/cantor-s3-on-local";
    private static final String BASE_DIRECTORY = "/tmp/cantor-archive-test";
    private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);

    private Cantor localCantor;
    private Cantor cantorOnS3;
    private FileArchiver archiver;

    @BeforeTest
    public void setup() throws IOException {
//        this.archiver = new FileArchiver("/tmp/akouthoofd-cantor-test");
//        this.localCantor = new ArchivableCantor(new CantorOnH2("/tmp/cantor-s3-on-local"), new FileArchiver());
//        generateData();
//
//        final AmazonS3 s3Client = createS3Client();
//        this.cantorOnS3 = new CantorOnS3(s3Client);
    }

    @Test(enabled = false)
    public void testObjectArchive() throws IOException {
        final Cantor cantorOnGrpc = new CantorOnGrpc("cantor.casp.prd-samtwo.prd.slb.sfdc.net:11983");
        this.archiver.eventsArchiver().archive(cantorOnGrpc.events(),
                "ui-http-request-log",
                1589401446000L - (3600000 * 24), 1589401446000L,
                null, null);

        final StreamingObjects subjects = (StreamingObjects) cantorOnS3.objects();
        final FileInputStream fileInputStream = new FileInputStream("/tmp/akouthoofd-cantor-test");
        subjects.store("cantor-archive-test", "prd-request-log", fileInputStream, fileInputStream.available());
//        Assert.assertTrue(cantorOnS3.objects().keys(CANTOR_S3_NAMESPACE, 0, -1).contains(CANTOR_H2_NAMESPACE), "Events were not stored in S3.");

        final InputStream stream = subjects.stream("cantor-archive-test", "prd-request-log");
        try (final FileOutputStream out = new FileOutputStream(new File("/tmp/test"))) {
            IOUtils.copy(stream, out);
        }

        final Cantor localCantor = new CantorOnH2("/tmp/cantor-s3-on-local");
        localCantor.events().create("something");
        this.archiver.eventsArchiver().restore(localCantor.events(), "something", 1589401446000L - (3600000 * 24), 1589401446000L);
        Assert.assertTrue(localCantor.events().get("something", 0, Long.MAX_VALUE, true).size() > 0, "Events were not restored");
    }

    private void generateData() throws IOException {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int namespaceCount = 0; namespaceCount < random.nextInt(2, 5); namespaceCount++) {
            final String namespace = "cantor-archive-test-" + Math.abs(UUID.randomUUID().hashCode());
            this.localCantor.events().create(namespace);
            CANTOR_H2_NAMESPACES.put(namespace, random.nextLong(TIMEFRAME_ORIGIN, TIMEFRAME_BOUND));

            for (int eventCount = 0; eventCount < random.nextInt(100, 1000); eventCount++) { // 1GB max
                final byte[] randomPayload = new byte[random.nextInt(0, 1_000_000)]; // 1MB max
                random.nextBytes(randomPayload);
                this.localCantor.events().store(
                        namespace, random.nextLong(TIMEFRAME_ORIGIN, TIMEFRAME_BOUND),
                        null,null, randomPayload
                );
            }
        }
    }

    private AmazonS3 createS3Client() throws IOException {
        final File s3File = new File("/Users/akouthoofd/coding/public/cantor/cantor-s3/src/test/resources/s3.json");
        final JsonNode jsonObject = mapper.readTree(s3File);

        final JsonNode keyId = jsonObject.get("AccessKeyID");
        final JsonNode accessKey = jsonObject.get("SecretAccessKey");
        final JsonNode sessionToken = jsonObject.get("SessionToken");

        final BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(keyId.asText(), accessKey.asText(), sessionToken.asText());
        return AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2)
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .build();
    }
}

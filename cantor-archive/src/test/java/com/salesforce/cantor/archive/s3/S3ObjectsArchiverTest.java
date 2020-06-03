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
import com.salesforce.cantor.misc.archivable.ArchivableCantor;
import com.salesforce.cantor.s3.CantorOnS3;
import com.salesforce.cantor.s3.StreamingObjects;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class S3ObjectsArchiverTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String CANTOR_S3_NAMESPACE = "cantor-archive-test";

    private static final String CANTOR_H2_NAMESPACE = "something";

    private Cantor localCantor;
    private Cantor cantorOnS3;
    private FileArchiver archiver;

    @BeforeTest
    public void setup() throws IOException {
        this.archiver = new FileArchiver("/tmp/akouthoofd-cantor-test");
        this.localCantor = new ArchivableCantor(new CantorOnH2("/tmp/cantor-s3-on-local"), new FileArchiver());
        restoreData();

        final AmazonS3 s3Client = createS3Client();
        this.cantorOnS3 = new CantorOnS3(s3Client);
        this.cantorOnS3.objects().delete(CANTOR_S3_NAMESPACE, CANTOR_H2_NAMESPACE);
    }

    @Test(enabled = false)
    public void testObjectArchive() throws IOException {

        final AmazonS3 s3Client = createS3Client();
        final CantorOnS3 cantorOnS3 = new CantorOnS3(s3Client);
        final Cantor cantorOnGrpc = new CantorOnGrpc("cantor.casp.prd-samtwo.prd.slb.sfdc.net:11983");
        this.archiver.eventsArchiver().archive(cantorOnGrpc.events(),
                "ui-http-request-log",
                1589401446000L - (3600000 * 24), 1589401446000L,
                null, null);

        final StreamingObjects subjects = (StreamingObjects) cantorOnS3.objects();
        final FileInputStream fileInputStream = new FileInputStream("/tmp/akouthoofd-cantor-test");
        subjects.store("cantor-archive-test", "prd-request-log", fileInputStream, fileInputStream.available());
        Assert.assertTrue(cantorOnS3.objects().keys(CANTOR_S3_NAMESPACE, 0, -1).contains(CANTOR_H2_NAMESPACE), "Events were not stored in S3.");

        final InputStream stream = subjects.stream("cantor-archive-test", "prd-request-log");
        try (final FileOutputStream out = new FileOutputStream(new File("/tmp/test"))) {
            IOUtils.copy(stream, out);
        }

        final Cantor localCantor = new CantorOnH2("/tmp/cantor-s3-on-local");
        localCantor.events().create("something");
        this.archiver.eventsArchiver().restore(localCantor.events(), "something", 1589401446000L - (3600000 * 24), 1589401446000L);
        Assert.assertTrue(localCantor.events().get("something", 0, Long.MAX_VALUE, true).size() > 0, "Events were not restored");
    }

    private void restoreData() throws IOException {
        this.localCantor.events().drop("something");
        this.localCantor.events().create("something");
        this.archiver.eventsArchiver().restore(this.localCantor.events(), "something", 1589401446000L - (3600000 * 24), 1589401446000L);
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

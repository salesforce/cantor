package com.salesforce.cantor.s3;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseEventsTest;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Scanner;

@Test(enabled = false)
public class EventsOnS3Test extends AbstractBaseEventsTest {
    private static final String credentialsLocation = "/path/to/creds";

    @Override
    protected Cantor getCantor() throws IOException {
        final AmazonS3 s3Client = createS3Client();
        return new CantorOnS3(s3Client, "default");
    }

    // insert real S3 client here to run integration testing
    private AmazonS3 createS3Client() throws IOException {
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
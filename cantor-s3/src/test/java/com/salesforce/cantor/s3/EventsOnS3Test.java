package com.salesforce.cantor.s3;

import com.adobe.testing.s3mock.testng.S3MockListener;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseEventsTest;
import org.testng.annotations.Listeners;

import java.io.*;
import java.util.Scanner;

@Listeners(value = { S3MockListener.class })
public class EventsOnS3Test extends AbstractBaseEventsTest {
    @Override
    protected Cantor getCantor() throws IOException {
        final AmazonS3 s3Client = S3Mock.getInstance().createS3Client("us-west-1");
        return new CantorOnS3(s3Client);
    }

    @Override
    public void testNamespaces() throws Exception {
        super.testNamespaces();
    }

    @Override
    public void testStore3kRandomEvents() throws Exception {
        super.testStore3kRandomEvents();
    }
}
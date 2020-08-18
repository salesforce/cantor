package com.salesforce.cantor.s3.single.bucket;

import com.adobe.testing.s3mock.testng.S3Mock;
import com.adobe.testing.s3mock.testng.S3MockListener;
import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;
import org.testng.annotations.Listeners;

import java.io.IOException;

@Listeners(value = { S3MockListener.class })
public class ObjectsOnS3SingleBucketTest extends AbstractBaseObjectsTest {

    @Override
    protected double getStoreMagnitude() {
        return 0.25;
    }

    @Override
    protected Cantor getCantor() throws IOException {
        final AmazonS3 s3Client = S3Mock.getInstance().createS3Client("us-west-1");
        return new CantorOnS3SingleBucket(s3Client);
    }
}

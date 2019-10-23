package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ObjectsOnS3Test {

    private AmazonS3 mockS3Client() {
        final Map<String, MockedBucket> bucketMock = new HashMap<>();
        final AmazonS3 s3Mock = mock(AmazonS3.class);

        // a bucket exists if it's been added to the map of mocked buckets
        when(s3Mock.doesBucketExistV2(anyString())).then((Answer<Boolean>) mockArg -> {
            final String bucketKey = mockArg.getArgument(0).toString();
            return bucketMock.containsKey(bucketKey);
        });

        // "create" the bucket by adding an empty list to the map of mocked buckets
        when(s3Mock.createBucket(anyString())).then(mockArg -> bucketMock.put(mockArg.getArgument(0).toString(), new MockedBucket(mockArg.getArgument(0).toString())));
        // list objects from the map of mocked buckets
        when(s3Mock.listObjects(anyString())).then(mockArg -> getObjectListing(bucketMock, mockArg.getArgument(0).toString()));
        //  delete objects
        doAnswer(invocation -> {

        }).when(s3Mock.deleteObject(anyString(), anyString()));
    }

    private ObjectListing getObjectListing(final Map<String, MockedBucket> bucketMock, final String bucketName) {
        final MockedBucket bucket = bucketMock.get(bucketName);
        if (bucket == null) {
            return null;
        }
        return new ObjectListing() {
            final int increments = Math.max(1, ThreadLocalRandom.current().nextInt(1, bucket.objects.size() / 5));
            int i = 0;

            @Override
            public boolean isTruncated() {
                return i < bucket.objects.size();
            }

            @Override
            public List<S3ObjectSummary> getObjectSummaries() {
                final int start = i;
                i = Math.min(bucket.objects.size(), i + increments);
                return new ArrayList<>(bucket.objects.subList(start, i));
            }
        };
    }

    private static class MockedBucket {
        final Bucket bucket;
        final List<S3ObjectSummary> objects;

        private MockedBucket(final String name) {
            this.bucket = new Bucket(name);
            this.objects = new ArrayList<>();
        }
    }
}
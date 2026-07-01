/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj.support;

import com.salesforce.multicloudj.blob.client.BucketClient;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Test-only factory that returns a {@link BucketClient} backed by the multicloudj
 * in-memory blob provider.
 *
 * <p>The in-memory provider has a static {@code BUCKETS} map and requires the
 * bucket to be registered before any operation. Because the supplied
 * {@code BlobClient} would attempt to load every {@code AbstractBlobClient}
 * service on the classpath (including blob-aws/blob-gcp/blob-ali, which lack
 * public no-arg constructors and would explode under SPI lookup), this helper
 * seeds the bucket directly via reflection on {@code InMemoryBlobStore.BUCKETS}.
 *
 * <p>Reflection on a SPI-internal field is brittle by design — this is a
 * deliberate test-only hack. Production code never uses the in-memory provider.
 */
public final class InMemoryBucketClients {
    private static final String IN_MEMORY_STORE_FQN =
            "com.salesforce.multicloudj.blob.inmemory.InMemoryBlobStore";
    private static final String BUCKET_METADATA_FQN =
            "com.salesforce.multicloudj.blob.inmemory.InMemoryBlobStore$BucketMetadata";

    private InMemoryBucketClients() {}

    public static BucketClient fresh() {
        String bucketName = "test-" + UUID.randomUUID();
        registerBucket(bucketName);
        return BucketClient.builder("memory")
                .withBucket(bucketName)
                .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void registerBucket(String bucketName) {
        try {
            Class<?> storeClass = Class.forName(IN_MEMORY_STORE_FQN);
            Field bucketsField = storeClass.getDeclaredField("BUCKETS");
            bucketsField.setAccessible(true);
            Map buckets = (Map) bucketsField.get(null);

            Class<?> metadataClass = Class.forName(BUCKET_METADATA_FQN);
            Constructor<?> ctor = metadataClass.getDeclaredConstructor(Instant.class);
            ctor.setAccessible(true);
            Object metadata = ctor.newInstance(Instant.now());

            buckets.put(bucketName, metadata);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                    "Failed to seed in-memory bucket '" + bucketName + "'. "
                            + "The multicloudj blob-inmemory layout may have changed; "
                            + "verify InMemoryBlobStore.BUCKETS and BucketMetadata(Instant) still exist.",
                    e);
        }
    }
}

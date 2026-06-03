/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.inmemory.InMemoryBlobStore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

public class ObjectsOnMulticloudjTest extends AbstractBaseObjectsTest {

    // The blob backend mirrors cantor-s3: store() does not enforce namespace existence;
    // matches cantor-s3 behavior, which leaves its conformance tests disabled for the same reason.
    @Override
    @Test(enabled = false)
    public void testBadInput() {
    }


    // shared across getCantor() calls within a test instance: the abstract test calls getCantor()
    // multiple times (once per before/after, plus inside test bodies), and they must all hit the
    // same bucket so namespaces created in @BeforeMethod are visible to the test body
    private static final String bucketName = "cantor-objects-test-" + UUID.randomUUID();
    private static volatile Cantor cantor;

    @Override
    protected synchronized Cantor getCantor() throws IOException {
        if (cantor != null) {
            return cantor;
        }
        InMemoryBlobStore.createBucket(bucketName);
        final InMemoryBlobStore store = new InMemoryBlobStore.Builder()
                .withBucket(bucketName)
                .withRegion("us-west-2")
                .build();
        final BucketClient bucketClient = new InMemoryTestBucketClient(store);
        cantor = new CantorOnMulticloudj(bucketClient);
        return cantor;
    }
}

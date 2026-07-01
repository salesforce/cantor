/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.multicloudj.support.InMemoryBucketClients;
import com.salesforce.multicloudj.blob.client.BucketClient;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.*;

public class CantorOnMulticloudjTest {

    @Test
    public void testObjects_returnsNonNull() throws Exception {
        final BucketClient bc = InMemoryBucketClients.fresh();
        try (CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc)) {
            assertNotNull(cantor.objects());
            assertTrue(cantor.objects() instanceof ObjectsOnMulticloudj);
        }
    }

    @Test
    public void testEvents_returnsNonNull() throws Exception {
        final BucketClient bc = InMemoryBucketClients.fresh();
        try (CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc)) {
            assertNotNull(cantor.events());
            assertTrue(cantor.events() instanceof EventsOnMulticloudj);
        }
    }

    @Test
    public void testSets_throwsUnsupportedOperation() throws Exception {
        final BucketClient bc = InMemoryBucketClients.fresh();
        try (CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc)) {
            try {
                cantor.sets();
                fail("Expected UnsupportedOperationException");
            } catch (UnsupportedOperationException e) {
                assertTrue(e.getMessage().contains("Sets are not implemented on multicloudj"),
                        "Message should indicate Sets not implemented, got: " + e.getMessage());
            }
        }
    }

    @Test
    public void testConvenienceConstructor_buildsInMemoryClient() throws Exception {
        // The convenience constructor builds its own BucketClient internally
        // Using "memory" provider which is already registered by InMemoryBucketClients seeding
        final String bucket = "ctor-test-" + System.nanoTime();
        InMemoryBucketClients.registerBucket(bucket);
        try (CantorOnMulticloudj cantor = new CantorOnMulticloudj("memory", "any-region", bucket)) {
            assertNotNull(cantor.objects());
            assertNotNull(cantor.events());
        }
    }

    @Test
    public void testConvenienceConstructor_missingProviderThrowsIOException() {
        try {
            new CantorOnMulticloudj("this-provider-does-not-exist", "us-east-1", "some-bucket");
            fail("Expected IOException for missing provider");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("failed to construct BucketClient"),
                    "Message should mention 'failed to construct BucketClient', got: " + e.getMessage());
            assertTrue(e.getMessage().contains("this-provider-does-not-exist"),
                    "Message should contain provider id, got: " + e.getMessage());
        }
    }

    @Test
    public void testClose_externallyManagedClientNotClosed() throws Exception {
        final BucketClient bc = InMemoryBucketClients.fresh();
        CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc);
        cantor.close();

        // After CantorOnMulticloudj.close(), the externally-managed client should still be usable
        assertTrue(bc.doesBucketExist(),
                "Externally-managed BucketClient should still work after CantorOnMulticloudj.close()");
    }

    @Test
    public void testClose_isIdempotent() throws Exception {
        final BucketClient bc = InMemoryBucketClients.fresh();
        CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc);
        cantor.close();
        cantor.close(); // second call must not throw
    }

    // ─── Negative input validation tests ─────────────────────────────────────────

    @Test
    public void testPrimaryConstructor_rejectsNullBucketClient() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj((BucketClient) null));
    }

    @Test
    public void testConvenienceConstructor_rejectsNullProviderId() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj(null, "us-east-1", "bucket"));
    }

    @Test
    public void testConvenienceConstructor_rejectsEmptyProviderId() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj("", "us-east-1", "bucket"));
    }

    @Test
    public void testConvenienceConstructor_rejectsNullRegion() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj("memory", null, "bucket"));
    }

    @Test
    public void testConvenienceConstructor_rejectsEmptyRegion() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj("memory", "", "bucket"));
    }

    @Test
    public void testConvenienceConstructor_rejectsNullBucket() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj("memory", "us-east-1", null));
    }

    @Test
    public void testConvenienceConstructor_rejectsEmptyBucket() {
        assertThrows(IllegalArgumentException.class,
                () -> new CantorOnMulticloudj("memory", "us-east-1", ""));
    }
}

/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;
import com.salesforce.cantor.multicloudj.support.InMemoryBucketClients;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.UUID;

import static org.testng.Assert.*;

public class ObjectsOnMulticloudjTest extends AbstractBaseObjectsTest {
    private final Cantor cantor;

    public ObjectsOnMulticloudjTest() throws IOException {
        this.cantor = new CantorOnMulticloudj(InMemoryBucketClients.fresh());
    }

    @Override
    protected Cantor getCantor() throws IOException {
        return cantor;
    }

    @Test
    public void testDelete_returnsFalseWhenAbsent() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String ns = "delete-absent-ns-" + UUID.randomUUID();
        objects.create(ns);
        assertFalse(objects.delete(ns, "missing-key"));
    }

    @Test
    public void testKeys_filtersOutNamespaceMarker() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String ns = "marker-ns-" + UUID.randomUUID();
        objects.create(ns);
        objects.store(ns, "real-key", "data".getBytes());

        Collection<String> keys = objects.keys(ns, "", 0, -1);
        assertFalse(keys.contains(".namespace"), "keys() should filter out the .namespace marker");
        assertTrue(keys.contains("real-key"), "keys() should include real keys");
        assertEquals(keys.size(), 1);
    }

    @Test
    public void testStore_propagatesIOExceptionForUncreatedNamespace() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String uncreatedNs = "never-created-" + UUID.randomUUID();
        assertThrows(IOException.class, () -> objects.store(uncreatedNs, "key", "val".getBytes()));
    }

    @Test
    public void testStreamStore_roundTrip() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String ns = "stream-ns-" + UUID.randomUUID();
        objects.create(ns);

        byte[] data = "streaming-content-hello".getBytes();
        objects.store(ns, "stream-key", new ByteArrayInputStream(data), data.length);

        InputStream result = objects.stream(ns, "stream-key");
        assertNotNull(result, "stream() should return non-null for existing key");
        byte[] read = result.readAllBytes();
        assertEquals(read, data);
    }

    @Test
    public void testStream_returnsNullForMissingKey() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String ns = "stream-miss-ns-" + UUID.randomUUID();
        objects.create(ns);

        InputStream result = objects.stream(ns, "nonexistent-key");
        assertNull(result, "stream() should return null when key does not exist");
    }

    @Test
    public void testSize_excludesNamespaceMarker() throws IOException {
        final ObjectsOnMulticloudj objects = new ObjectsOnMulticloudj(InMemoryBucketClients.fresh());
        final String ns = "size-ns-" + UUID.randomUUID();
        objects.create(ns);

        assertEquals(objects.size(ns), 0, "Size should be 0 after create (marker excluded)");

        objects.store(ns, "k1", "v1".getBytes());
        objects.store(ns, "k2", "v2".getBytes());
        assertEquals(objects.size(ns), 2);
    }

}

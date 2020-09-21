/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common.performance;

import com.salesforce.cantor.Objects;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

public abstract class AbstractBaseObjectsPerformanceTest extends AbstractBaseCantorPerformanceTest {
    private final String namespace = UUID.randomUUID().toString();

    @BeforeMethod
    public void before() throws Exception {
        getObjects().create(this.namespace);
    }

    @AfterMethod
    public void after() throws Exception {
        getObjects().drop(this.namespace);
    }

    // override to store less (for impls that storing is more expensive) by setting to less than 1
    protected double getStoreMagnitude() {
        return 1.0D;
    }

    @Test
    public void testBadInput() throws Exception {
        final Objects objects = getObjects();

        assertThrows(IllegalArgumentException.class, () -> objects.store(this.namespace, null, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> objects.store(this.namespace, "", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> objects.store(this.namespace, "abc", null));

        assertThrows(IllegalArgumentException.class, () -> objects.get(this.namespace, (String) null));
        assertThrows(IllegalArgumentException.class, () -> objects.get(this.namespace, ""));
        assertThrows(IllegalArgumentException.class, () -> objects.get(this.namespace, (Collection<String>) null));

        assertThrows(IllegalArgumentException.class, () -> objects.delete(this.namespace, (String) null));
        assertThrows(IllegalArgumentException.class, () -> objects.delete(this.namespace, ""));
        assertThrows(IllegalArgumentException.class, () -> objects.delete(this.namespace, (Collection<String>) null));

        // trying to store an object in a this.namespace that is not created yet should throw ioexception
        assertThrows(IOException.class, () -> objects.store(UUID.randomUUID().toString(), "foo", "bar".getBytes()));
    }

    @Test
    public void testNamespaces() throws Exception {
        final Objects objects = getObjects();
        final List<String> namespaces = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            final String namespace = UUID.randomUUID().toString();
            namespaces.add(namespace);
            assertFalse(objects.namespaces().contains(namespace));

            objects.create(namespace);
            assertTrue(objects.namespaces().contains(namespace));
        }

        for (final String namespace : namespaces) {
            objects.drop(namespace);
            assertFalse(objects.namespaces().contains(namespace));
        }
    }

    @Test
    public void testStoreGetDelete() throws Exception {
        final Objects objects = getObjects();

        final String key = UUID.randomUUID().toString();
        final byte[] value = UUID.randomUUID().toString().getBytes();

        assertNull(objects.get(this.namespace, key));
        assertFalse(objects.delete(this.namespace, key));
        objects.store(this.namespace, key, value);
        assertEquals(value, objects.get(this.namespace, key));
        assertTrue(objects.delete(this.namespace, key));
        assertNull(objects.get(this.namespace, key));

        final Map<String, byte[]> batch = new HashMap<>(100);
        storeRandom(objects, this.namespace, batch, 100);

        for (final String k : batch.keySet()) {
            assertEquals(batch.get(k), objects.get(this.namespace, k));
        }

        objects.delete(this.namespace, batch.keySet());
        for (final String k : batch.keySet()) {
            assertNull(objects.get(this.namespace, k));
        }
    }

    @Test
    public void testStoreGetBatch() throws Exception {
        final Objects objects = getObjects();

        final Map<String, byte[]> empty = objects.get(this.namespace, Collections.emptyList());
        assertTrue(empty.isEmpty());

        final Map<String, byte[]> kvs = new HashMap<>();
        for (int i = 0; i < 100; ++i) {
            final String key = UUID.randomUUID().toString();
            final byte[] value = UUID.randomUUID().toString().getBytes();
            kvs.put(key, value);
        }

        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertNull(objects.get(this.namespace, entry.getKey()));
        }
        objects.store(this.namespace, kvs);
        final Map<String, byte[]> results = objects.get(this.namespace, kvs.keySet());
        assertEquals(kvs.size(), results.size());
        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertEquals(results.get(entry.getKey()), entry.getValue());
        }
        objects.delete(this.namespace, kvs.keySet());
        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertNull(objects.get(this.namespace, entry.getKey()));
        }
    }

    @Test
    public void testStoreKeys() throws Exception {
        final Objects objects = getObjects();

        final Map<String, byte[]> empty = objects.get(this.namespace, Collections.emptyList());
        assertTrue(empty.isEmpty());

        final Map<String, byte[]> kvs = new HashMap<>();
        for (int i = 0; i < 100; ++i) {
            final String key = UUID.randomUUID().toString();
            final byte[] value = UUID.randomUUID().toString().getBytes();
            kvs.put(key, value);
        }

        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertNull(objects.get(this.namespace, entry.getKey()));
        }
        objects.store(this.namespace, kvs);
        final int count = ThreadLocalRandom.current().nextInt(1, 99);
        final Collection<String> partialResults = objects.keys(this.namespace, 0, count);
        assertEquals(partialResults.size(), count);

        final Collection<String> results = objects.keys(this.namespace, 0, -1);
        assertEquals(kvs.size(), results.size());
        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertTrue(results.contains(entry.getKey()));
        }
        objects.delete(this.namespace, kvs.keySet());
        for (final Map.Entry<String, byte[]> entry : kvs.entrySet()) {
            assertNull(objects.get(this.namespace, entry.getKey()));
        }
    }

    @Test
    public void testSize() throws Exception {
        final Objects objects = getObjects();

        // delete everything, should leave 0 objects
        objects.create(this.namespace);
        objects.drop(this.namespace);
        objects.create(this.namespace);

        assertEquals(objects.size(this.namespace), 0);

        // store and check size
        final Map<String, byte[]> batch = new HashMap<>();
        storeRandom(objects, this.namespace, batch, 1_000);
        assertEquals(objects.size(this.namespace), batch.size());

        // delete and check size
        int removed = 0;
        for (final String key : batch.keySet()) {
            if (removed >= batch.size() / 2) {
                break;
            }
            objects.delete(this.namespace, key);
            removed++;
        }
        assertEquals(objects.size(this.namespace), batch.size() - removed);

        // delete and check again
        objects.delete(this.namespace, batch.keySet());
        assertEquals(objects.size(this.namespace), 0);
    }

    private void storeRandom(final Objects objects,
                             final String namespace,
                             final Map<String, byte[]> map,
                             final int times) throws IOException {
        final double storeCount = Math.floor(times * getStoreMagnitude());
        for (int i = 0; i < storeCount; i++) {
            final String key = UUID.randomUUID().toString();
            final byte[] value = UUID.randomUUID().toString().getBytes();
            map.put(key, value);
            objects.store(namespace, key, value);
        }
    }

    private Objects getObjects() throws IOException {
        return getCantor().objects();
    }

}

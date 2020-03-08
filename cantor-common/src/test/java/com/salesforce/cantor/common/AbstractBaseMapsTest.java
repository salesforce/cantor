/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import com.salesforce.cantor.Maps;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.testng.Assert.*;

public abstract class AbstractBaseMapsTest extends AbstractBaseCantorTest {
    private final String namespace = UUID.randomUUID().toString();

    @BeforeMethod
    public void before() throws Exception {
        getMaps().create(this.namespace);
    }

    @AfterMethod
    public void after() throws Exception {
        getMaps().drop(this.namespace);
    }

    @Test
    public void testBadInput() throws Exception {
        final Maps maps = getMaps();

        assertThrows(IllegalArgumentException.class, () -> maps.store(this.namespace, null));
        assertThrows(IllegalArgumentException.class, () -> maps.delete(this.namespace, null));
    }

    @Test
    public void testNamespaces() throws Exception {
        final Maps maps = getMaps();
        final List<String> namespaces = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            final String namespace = UUID.randomUUID().toString();
            namespaces.add(namespace);
            assertFalse(maps.namespaces().contains(namespace));

            maps.create(namespace);
            assertTrue(maps.namespaces().contains(namespace));
        }

        for (final String namespace : namespaces) {
            maps.drop(namespace);
            assertFalse(maps.namespaces().contains(namespace));
        }
    }

    @Test
    public void testStoreGetDelete() throws Exception {
        final Maps maps = getMaps();

        final Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 100; ++i) {
            final String key = String.format("key-%d", i);
            final String value = UUID.randomUUID().toString();
            map.put(key, value);
        }

        maps.store(this.namespace, map);
        final Collection<Map<String, String>> results = maps.get(this.namespace, map);
        assertEquals(results.size(), 1);
        assertEquals(results.iterator().next(), map);

        maps.delete(this.namespace, map);
        assertTrue(maps.get(this.namespace, map).isEmpty());
    }

    private Maps getMaps() throws IOException {
        return getCantor().maps();
    }

}

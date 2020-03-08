/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapsPreconditions extends CommonPreconditions {

    public static void checkStore(final String namespace, final Map<String, String> map) {
        checkNamespace(namespace);
        checkMap(map);
    }

    public static void checkGet(final String namespace, final Map<String, String> query) {
        checkNamespace(namespace);
        checkQuery(query);
    }

    public static void checkDelete(final String namespace, final Map<String, String> query) {
        checkNamespace(namespace);
        checkQuery(query);
    }

    private static void checkMap(final Map<String, String> map) {
        checkArgument(map != null && !map.isEmpty(), "null/empty map");
        checkArgument(map.keySet().size() <= 100, "map contains more than 100 keys");
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "map '" + key + "' has null/empty value");
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == map.size(), "map keys must be unique ignoring case");
    }

    private static void checkQuery(final Map<String, String> query) {
        checkArgument(query != null && !query.isEmpty(), "null/empty query");
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : query.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "query '" + key + "' has null/empty value");
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == query.size(), "query keys must be unique ignoring case");
    }
}


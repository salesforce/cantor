/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import java.util.Collection;
import java.util.Map;

public class ObjectsPreconditions extends CommonPreconditions {

    public static void checkStore(final String namespace, final String key, final byte[] bytes) {
        checkNamespace(namespace);
        checkString(key, namespace);
        checkArgument(bytes != null, "null value", namespace);
    }

    public static void checkStore(final String namespace, final Map<String, byte[]> batch) {
        checkNamespace(namespace);
        checkArgument(batch != null, "null batch", namespace);
    }

    public static void checkGet(final String namespace, final int start, final int count) {
        checkNamespace(namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkKeys(final String namespace, final int start, final int count) {
        checkNamespace(namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkKeys(final String namespace, final int start, final int count, final String prefix) {
        checkKeys(namespace, start, count);
        checkString(prefix, namespace);
    }

    public static void checkGet(final String namespace, final String key) {
        checkNamespace(namespace);
        checkString(key, namespace);
    }

    public static void checkGet(final String namespace, final Collection<String> keys) {
        checkNamespace(namespace);
        checkArgument(keys != null, "null entries", namespace);
    }

    public static void checkDelete(final String namespace, final String key) {
        checkNamespace(namespace);
        checkString(key, namespace);
    }

    public static void checkSize(final String namespace) {
        checkNamespace(namespace);
    }

    public static void checkDelete(final String namespace, final Collection<String> keys) {
        checkNamespace(namespace);
        checkArgument(keys != null, "null entries", namespace);
    }
}

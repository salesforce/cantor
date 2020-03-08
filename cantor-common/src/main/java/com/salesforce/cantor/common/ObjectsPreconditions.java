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
        checkString(key);
        checkArgument(bytes != null, "null value");
    }

    public static void checkStore(final String namespace, final Map<String, byte[]> batch) {
        checkNamespace(namespace);
        checkArgument(batch != null, "null batch");
    }

    public static void checkGet(final String namespace, final int start, final int count) {
        checkNamespace(namespace);
        checkArgument(start >= 0, "invalid start");
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count");
    }

    public static void checkKeys(final String namespace, final int start, final int count) {
        checkNamespace(namespace);
        checkArgument(start >= 0, "invalid start");
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count");
    }

    public static void checkGet(final String namespace, final String key) {
        checkNamespace(namespace);
        checkString(key);
    }

    public static void checkGet(final String namespace, final Collection<String> keys) {
        checkNamespace(namespace);
        checkArgument(keys != null, "null entries");
    }

    public static void checkDelete(final String namespace, final String key) {
        checkNamespace(namespace);
        checkString(key);
    }

    public static void checkSize(final String namespace) {
        checkNamespace(namespace);
    }

    public static void checkDelete(final String namespace, final Collection<String> keys) {
        checkNamespace(namespace);
        checkArgument(keys != null, "null entries");
    }
}

/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import java.util.Collection;
import java.util.Map;

public class SetsPreconditions extends CommonPreconditions {

    public static void checkGet(final String namespace,
                                final String set,
                                final long min,
                                final long max,
                                final int start,
                                final int count,
                                final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkUnion(final String namespace,
                                  final Collection<String> sets,
                                  final long min,
                                  final long max,
                                  final int start,
                                  final int count,
                                  final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkArgument(sets != null && !sets.isEmpty(), "null/empty sets", namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkIntersect(final String namespace,
                                      final Collection<String> sets,
                                      final long min,
                                      final long max,
                                      final int start,
                                      final int count,
                                      final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkArgument(sets != null && !sets.isEmpty(), "null/empty sets", namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkPop(final String namespace,
                                final String set,
                                final long min,
                                final long max,
                                final int start,
                                final int count,
                                final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkAdd(final String namespace, final String set, final String entry, final long weightIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkString(entry, namespace);
    }

    public static void checkAdd(final String namespace, final String set, final Map<String, Long> entries) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(entries != null, "null entries", namespace);
    }

    public static void checkDelete(final String namespace, final String set, final long min, final long max) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
    }

    public static void checkDelete(final String namespace, final String set, final String entry) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkString(entry, namespace);
    }

    public static void checkDelete(final String namespace, final String set, final Collection<String> entries) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(entries != null, "null entries", namespace);
    }

    public static void checkSets(final String namespace) {
        checkNamespace(namespace);
    }

    public static void checkEntries(final String namespace,
                                    final String set,
                                    final long min,
                                    final long max,
                                    final int start,
                                    final int count,
                                    final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkKeys(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascendingIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkArgument(min <= max, "invalid min/max", namespace);
        checkArgument(start >= 0, "invalid start", namespace);
        checkArgument(count >= 0 || (count == -1 && start == 0), "invalid count", namespace);
    }

    public static void checkSize(final String namespace, final String set) {
        checkNamespace(namespace);
        checkString(set, namespace);
    }

    public static void checkWeight(final String namespace, final String set, final String entry) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkString(entry, namespace);
    }

    public static void checkInc(final String namespace, final String set, final String entry, final long countIgnored) {
        checkNamespace(namespace);
        checkString(set, namespace);
        checkString(entry, namespace);
    }
}

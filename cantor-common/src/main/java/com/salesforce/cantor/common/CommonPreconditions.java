/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

public class CommonPreconditions {

    public static final int MAX_NAMESPACE_LENGHT = 512;

    public static void checkString(final String string) {
        checkString(string, "null/empty string");
    }

    public static void checkString(final String string, final int maxLength) {
        checkString(string, "null/empty string");
        checkArgument(string.length() <= maxLength, "string cannot be longer than " + maxLength + " characters");
    }

    public static void checkString(final String string, final String message) {
        checkArgument(string != null && string.length() > 0, message);
    }

    public static void checkArgument(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void checkState(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public static void checkNamespace(final String namespace) {
        checkString(namespace, "null/empty namespace");
        checkArgument(namespace.length() < MAX_NAMESPACE_LENGHT,
                "namespace longer than " + MAX_NAMESPACE_LENGHT + " chars"
        );
    }

    public static void checkCreate(final String namespace) {
        checkNamespace(namespace);
    }

    public static void checkDrop(final String namespace) {
        checkNamespace(namespace);
    }
}

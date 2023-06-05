/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.EventsPreconditions.*;
import static org.testng.Assert.assertThrows;

public class EventsPreconditionsTest {

    @Test
    public void testCheckTimestamps() {
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(-1, 1, namespace));
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(100, 99, namespace));
        final long now = System.currentTimeMillis();
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(now - TimeUnit.DAYS.toMillis(7) - 1, now, namespace));
        checkTimestamps(0, 0, namespace);
        checkTimestamps(0, 1, namespace);
    }

    @Test
    public void testChecks() {
        final Map<String, String> nullMetadata = new HashMap<>();
        nullMetadata.put("foo", null);
        assertThrows(IllegalArgumentException.class, () -> checkMetadata(nullMetadata, namespace));
        assertThrows(IllegalArgumentException.class, () -> checkMetadataQuery(nullMetadata, namespace));
        assertThrows(IllegalArgumentException.class, () -> checkDimensionsQuery(nullMetadata, namespace));

        final Map<String, String> validMetadata = new HashMap<>();
        validMetadata.put("foo", "bar");
        validMetadata.put("!@#$%&^(*&)*)(^) ?><?.,`~0123456789012345678901234567890123456789012345678901234567890123", "test");
        checkMetadata(validMetadata, namespace);
        checkMetadataQuery(validMetadata, namespace);
        checkDimensionsQuery(validMetadata, namespace);

        final Map<String, String> duplicateKeys = new HashMap<>();
        duplicateKeys.put("foo", UUID.randomUUID().toString());
        duplicateKeys.put("FOO", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> checkMetadata(duplicateKeys, namespace));
        assertThrows(IllegalArgumentException.class, () -> checkDimensionsQuery(duplicateKeys, namespace));
    }

    @Test
    public void testCheckDimensions() {
        final Map<String, Double> nullDimensions = new HashMap<>();
        nullDimensions.put("foo", null);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(nullDimensions, namespace));

        final Map<String, Double> nanDimensions = new HashMap<>();
        nanDimensions.put("foo", Double.NaN);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(nanDimensions, namespace));

        final Map<String, Double> infiniteDimensions = new HashMap<>();
        infiniteDimensions.put("foo", Double.POSITIVE_INFINITY);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(infiniteDimensions, namespace));

        final Map<String, Double> ninfiniteDimensions = new HashMap<>();
        ninfiniteDimensions.put("foo", Double.NEGATIVE_INFINITY);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(ninfiniteDimensions, namespace));

        final Map<String, Double> validDimensions = new HashMap<>();
        validDimensions.put("foo", ThreadLocalRandom.current().nextDouble());
        validDimensions.put("!@#$%&^(*&)*)(^) ?><?.,`~0123456789012345678901234567890123456789012345678901234567890123",
                ThreadLocalRandom.current().nextDouble());
        checkDimensions(validDimensions, namespace);
    }
}
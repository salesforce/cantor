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
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(100, 99));
        final long now = System.currentTimeMillis();
        assertThrows(IllegalArgumentException.class, () -> checkTimestamps(now - TimeUnit.DAYS.toMillis(7) - 1, now));
        checkTimestamps(0, 0);
        checkTimestamps(0, 1);
    }

    @Test
    public void testChecks() {
        final Map<String, String> nullMetadata = new HashMap<>();
        nullMetadata.put("foo", null);
        assertThrows(IllegalArgumentException.class, () -> checkMetadata(nullMetadata));
        assertThrows(IllegalArgumentException.class, () -> checkMetadataQuery(nullMetadata));
        assertThrows(IllegalArgumentException.class, () -> checkDimensionsQuery(nullMetadata));

        final Map<String, String> validMetadata = new HashMap<>();
        validMetadata.put("foo", "bar");
        validMetadata.put("!@#$%&^(*&)*)(^) ?><?.,`~0123456789012345678901234567890123456789012345678901234567890123", "test");
        checkMetadata(validMetadata);
        checkMetadataQuery(validMetadata);
        checkDimensionsQuery(validMetadata);

        final Map<String, String> duplicateKeys = new HashMap<>();
        duplicateKeys.put("foo", UUID.randomUUID().toString());
        duplicateKeys.put("FOO", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> checkMetadata(duplicateKeys));
        assertThrows(IllegalArgumentException.class, () -> checkDimensionsQuery(duplicateKeys));
    }

    @Test
    public void testCheckDimensions() {
        final Map<String, Double> nullDimensions = new HashMap<>();
        nullDimensions.put("foo", null);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(nullDimensions));

        final Map<String, Double> nanDimensions = new HashMap<>();
        nanDimensions.put("foo", Double.NaN);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(nanDimensions));

        final Map<String, Double> infiniteDimensions = new HashMap<>();
        infiniteDimensions.put("foo", Double.POSITIVE_INFINITY);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(infiniteDimensions));

        final Map<String, Double> ninfiniteDimensions = new HashMap<>();
        ninfiniteDimensions.put("foo", Double.NEGATIVE_INFINITY);
        assertThrows(IllegalArgumentException.class, () -> checkDimensions(ninfiniteDimensions));

        final Map<String, Double> validDimensions = new HashMap<>();
        validDimensions.put("foo", ThreadLocalRandom.current().nextDouble());
        validDimensions.put("!@#$%&^(*&)*)(^) ?><?.,`~0123456789012345678901234567890123456789012345678901234567890123",
                ThreadLocalRandom.current().nextDouble());
        checkDimensions(validDimensions);
    }
}
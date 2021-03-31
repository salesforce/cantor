/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import com.salesforce.cantor.Events;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class EventsPreconditions extends CommonPreconditions {

    public static void checkStore(final String namespace, final Collection<Events.Event> batch) {
        checkNamespace(namespace);
        checkArgument(batch.size() <= 10_000, "batch larger than 10K events");
        for (final Events.Event event : batch) {
            checkStore(event.getTimestampMillis(), event.getMetadata(), event.getDimensions());
        }
    }

    public static void checkGet(final String namespace,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) {
        checkNamespace(namespace);
        checkTimestamps(startTimestampMillis, endTimestampMillis);
        checkMetadataQuery(metadataQuery);
        checkDimensionsQuery(dimensionsQuery);
    }

    public static void checkMetadata(final String namespace,
                                     final String metadataKey,
                                     final long startTimestampMillis,
                                     final long endTimestampMillis,
                                     final Map<String, String> metadataQuery,
                                     final Map<String, String> dimensionsQuery) {
        checkNamespace(namespace);
        checkString(metadataKey);
        checkTimestamps(startTimestampMillis, endTimestampMillis);
        checkMetadataQuery(metadataQuery);
        checkDimensionsQuery(dimensionsQuery);
    }

    public static void checkExpire(final String namespace, final long endTimestampMillis) {
        checkNamespace(namespace);
        checkArgument(endTimestampMillis >= 0, "invalid end timestamp");
    }

    static void checkTimestamps(final long startTimestampMillis, final long endTimestampMillis) {
        checkArgument(startTimestampMillis >= 0, "invalid start timestamp");
        checkArgument(endTimestampMillis >= startTimestampMillis, "end timestamp cannot be before start timestamp");
        checkArgument(endTimestampMillis - startTimestampMillis < TimeUnit.DAYS.toMillis(1), "query cannot be wider than 24 hours");
    }

    static void checkMetadata(final Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return;
        }
        checkArgument(metadata.keySet().size() <= 100, "metadata contains more than 100 keys");
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : metadata.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "metadata '" + key + "' has null/empty value");
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == metadata.size(), "metadata keys must be unique ignoring case");
    }

    static void checkDimensions(final Map<String, Double> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return;
        }
        checkArgument(dimensions.keySet().size() <= 400, "dimensions contains more than 400 keys");
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, Double> entry : dimensions.entrySet()) {
            final String key = entry.getKey();
            Double value = entry.getValue();
            checkArgument(value != null, "dimension '" + key + "' has null value");
            checkArgument(!Double.isNaN(value) && !Double.isInfinite(value), "dimension '" + key + "' is not a valid number");
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == dimensions.size(), "dimension keys must be unique ignoring case");
    }

    static void checkMetadataQuery(final Map<String, String> metadataQuery) {
        if (metadataQuery == null || metadataQuery.isEmpty()) {
            return;
        }
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "metadata query '" + key + "' has null/empty value");
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == metadataQuery.size(), "metadata keys must be unique ignoring case");
    }

    static void checkDimensionsQuery(final Map<String, String> dimensionsQuery) {
        if (dimensionsQuery == null || dimensionsQuery.isEmpty()) {
            return;
        }
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : dimensionsQuery.entrySet()) {
            checkArgument(entry.getValue() != null, "dimension '" + entry.getKey() + "' has null value");
            uniqueKeys.add(entry.getKey().toUpperCase());
        }
        checkArgument(uniqueKeys.size() == dimensionsQuery.size(), "dimension keys must be unique ignoring case");
    }

    private static void checkStore(final long timestampMillis,
                                   final Map<String, String> metadata,
                                   final Map<String, Double> dimensions) {
        checkArgument(timestampMillis >= 0, "invalid timestamp");
        checkMetadata(metadata);
        checkDimensions(dimensions);
    }

}


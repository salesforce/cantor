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
        checkArgument(batch.size() <= 10_000, "batch larger than 10K events", namespace);
        for (final Events.Event event : batch) {
            checkStore(event.getTimestampMillis(), event.getMetadata(), event.getDimensions(), namespace);
        }
    }

    public static void checkGet(final String namespace,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) {
        checkNamespace(namespace);
        checkTimestamps(startTimestampMillis, endTimestampMillis, namespace);
        checkMetadataQuery(metadataQuery, namespace);
        checkDimensionsQuery(dimensionsQuery, namespace);
    }

    public static void checkMetadata(final String namespace,
                                     final String metadataKey,
                                     final long startTimestampMillis,
                                     final long endTimestampMillis,
                                     final Map<String, String> metadataQuery,
                                     final Map<String, String> dimensionsQuery) {
        checkNamespace(namespace);
        checkString(metadataKey, namespace);
        checkTimestamps(startTimestampMillis, endTimestampMillis, namespace);
        checkMetadataQuery(metadataQuery, namespace);
        checkDimensionsQuery(dimensionsQuery, namespace);
    }

    public static void checkDimension(final String namespace,
                                      final String dimensionKey,
                                      final long startTimestampMillis,
                                      final long endTimestampMillis,
                                      final Map<String, String> metadataQuery,
                                      final Map<String, String> dimensionsQuery) {
        checkNamespace(namespace);
        checkString(dimensionKey, namespace);
        checkTimestamps(startTimestampMillis, endTimestampMillis, namespace);
        checkMetadataQuery(metadataQuery, namespace);
        checkDimensionsQuery(dimensionsQuery, namespace);
    }

    public static void checkExpire(final String namespace, final long endTimestampMillis) {
        checkNamespace(namespace);
        checkArgument(endTimestampMillis >= 0, "invalid end timestamp", namespace);
    }

    static void checkTimestamps(final long startTimestampMillis, final long endTimestampMillis, final String namespace) {
        checkArgument(startTimestampMillis >= 0, "invalid start timestamp", namespace);
        checkArgument(endTimestampMillis >= startTimestampMillis, "end timestamp cannot be before start timestamp", namespace);
        checkArgument(endTimestampMillis - startTimestampMillis <= TimeUnit.DAYS.toMillis(7), "query cannot be wider than 7 days", namespace);
    }

    static void checkMetadata(final Map<String, String> metadata, final String namespace) {
        if (metadata == null || metadata.isEmpty()) {
            return;
        }
        checkArgument(metadata.keySet().size() <= 100, "metadata contains more than 100 keys", namespace);
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : metadata.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "metadata '" + key + "' has null/empty value", namespace);
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == metadata.size(), "metadata keys must be unique ignoring case", namespace);
    }

    static void checkDimensions(final Map<String, Double> dimensions, final String namespace) {
        if (dimensions == null || dimensions.isEmpty()) {
            return;
        }
        checkArgument(dimensions.keySet().size() <= 100, "dimensions contains more than 100 keys", namespace);
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, Double> entry : dimensions.entrySet()) {
            final String key = entry.getKey();
            Double value = entry.getValue();
            checkArgument(value != null, "dimension '" + key + "' has null value", namespace);
            checkArgument(!Double.isNaN(value) && !Double.isInfinite(value), "dimension '" + key + "' is not a valid number", namespace);
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == dimensions.size(), "dimension keys must be unique ignoring case", namespace);
    }

    static void checkMetadataQuery(final Map<String, String> metadataQuery, final String namespace) {
        if (metadataQuery == null || metadataQuery.isEmpty()) {
            return;
        }
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String key = entry.getKey();
            checkString(entry.getValue(), "metadata query '" + key + "' has null/empty value", namespace);
            uniqueKeys.add(key.toUpperCase());
        }
        checkArgument(uniqueKeys.size() == metadataQuery.size(), "metadata keys must be unique ignoring case", namespace);
    }

    static void checkDimensionsQuery(final Map<String, String> dimensionsQuery, final String namespace) {
        if (dimensionsQuery == null || dimensionsQuery.isEmpty()) {
            return;
        }
        final Set<String> uniqueKeys = new HashSet<>();
        for (final Map.Entry<String, String> entry : dimensionsQuery.entrySet()) {
            checkArgument(entry.getValue() != null, "dimension '" + entry.getKey() + "' has null value", namespace);
            uniqueKeys.add(entry.getKey().toUpperCase());
        }
        checkArgument(uniqueKeys.size() == dimensionsQuery.size(), "dimension keys must be unique ignoring case", namespace);
    }

    private static void checkStore(final long timestampMillis,
                                   final Map<String, String> metadata,
                                   final Map<String, Double> dimensions,
                                   final String namespace) {
        checkArgument(timestampMillis >= 0, "invalid timestamp", namespace);
        checkMetadata(metadata, namespace);
        checkDimensions(dimensions, namespace);
    }

}


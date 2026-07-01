/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.cantor.multicloudj.support.InMemoryBucketClients;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class MulticloudjUtilsTest {

    // ─── matchesMetadata tests ───────────────────────────────────────────────────

    @DataProvider(name = "metadataMatchCases")
    public Object[][] metadataMatchCases() {
        return new Object[][] {
            // { metadata map, query map, expected result }
            // empty query matches everything
            { Map.of("host", "web-01"), Collections.emptyMap(), true },
            // implicit equality (no prefix)
            { Map.of("host", "web-01"), Map.of("host", "web-01"), true },
            // implicit equality miss
            { Map.of("host", "web-01"), Map.of("host", "web-02"), false },
            // explicit = operator
            { Map.of("env", "prod"), Map.of("env", "=prod"), true },
            // != operator
            { Map.of("env", "prod"), Map.of("env", "!=staging"), true },
            // != operator miss (value equals)
            { Map.of("env", "prod"), Map.of("env", "!=prod"), false },
            // ~ (LIKE) with trailing wildcard
            { Map.of("host", "web-01"), Map.of("host", "~web*"), true },
            // ~ (LIKE) with leading wildcard
            { Map.of("host", "web-01"), Map.of("host", "~*-01"), true },
            // ~ (LIKE) miss
            { Map.of("host", "web-01"), Map.of("host", "~api*"), false },
            // !~ (NOT LIKE)
            { Map.of("host", "web-01"), Map.of("host", "!~api*"), true },
            // !~ miss (pattern matches)
            { Map.of("host", "web-01"), Map.of("host", "!~web*"), false },
            // multi-key AND: both match
            { Map.of("host", "web-01", "env", "prod"), Map.of("host", "web-01", "env", "prod"), true },
            // multi-key AND: one misses
            { Map.of("host", "web-01", "env", "prod"), Map.of("host", "web-01", "env", "staging"), false },
            // query key not in metadata
            { Map.of("host", "web-01"), Map.of("region", "us-west"), false },
        };
    }

    @Test(dataProvider = "metadataMatchCases")
    public void testMatchesMetadata(Map<String, String> metadata, Map<String, String> query, boolean expected) {
        assertEquals(MulticloudjUtils.matchesMetadata(metadata, query), expected);
    }

    // ─── matchesDimensions tests ─────────────────────────────────────────────────

    @DataProvider(name = "dimensionMatchCases")
    public Object[][] dimensionMatchCases() {
        return new Object[][] {
            // { dimensions map, query map, expected result }
            // empty query matches everything
            { Map.of("cpu", 0.5), Collections.emptyMap(), true },
            // implicit equality
            { Map.of("cpu", 0.5), Map.of("cpu", "0.5"), true },
            // implicit equality miss
            { Map.of("cpu", 0.5), Map.of("cpu", "0.6"), false },
            // explicit = operator
            { Map.of("cpu", 42.0), Map.of("cpu", "=42.0"), true },
            // != operator
            { Map.of("cpu", 42.0), Map.of("cpu", "!=43.0"), true },
            // != operator miss
            { Map.of("cpu", 42.0), Map.of("cpu", "!=42.0"), false },
            // > operator
            { Map.of("cpu", 0.8), Map.of("cpu", ">0.5"), true },
            // > operator miss (equal)
            { Map.of("cpu", 0.5), Map.of("cpu", ">0.5"), false },
            // >= operator
            { Map.of("cpu", 0.5), Map.of("cpu", ">=0.5"), true },
            // < operator
            { Map.of("cpu", 0.3), Map.of("cpu", "<0.5"), true },
            // < operator miss
            { Map.of("cpu", 0.5), Map.of("cpu", "<0.5"), false },
            // <= operator
            { Map.of("cpu", 0.5), Map.of("cpu", "<=0.5"), true },
            // .. (BETWEEN inclusive)
            { Map.of("cpu", 0.5), Map.of("cpu", "0.0..1.0"), true },
            // .. at boundaries
            { Map.of("cpu", 0.0), Map.of("cpu", "0.0..1.0"), true },
            { Map.of("cpu", 1.0), Map.of("cpu", "0.0..1.0"), true },
            // .. miss (out of range)
            { Map.of("cpu", 1.5), Map.of("cpu", "0.0..1.0"), false },
            // multi-key AND: both match
            { Map.of("cpu", 0.5, "mem", 1024.0), Map.of("cpu", ">=0.0", "mem", "<=2048.0"), true },
            // multi-key AND: one misses
            { Map.of("cpu", 0.5, "mem", 4096.0), Map.of("cpu", ">=0.0", "mem", "<=2048.0"), false },
            // query key not in dimensions
            { Map.of("cpu", 0.5), Map.of("disk", ">0.0"), false },
        };
    }

    @Test(dataProvider = "dimensionMatchCases")
    public void testMatchesDimensions(Map<String, Double> dimensions, Map<String, String> query, boolean expected) {
        assertEquals(MulticloudjUtils.matchesDimensions(dimensions, query), expected);
    }

    // ─── listKeysOrdered tests ───────────────────────────────────────────────────

    @Test
    public void listKeysOrdered_pagination_returnsStableOrder() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String prefix = "test-prefix/";
        final int totalKeys = 1000;

        for (int i = 0; i < totalKeys; i++) {
            String key = prefix + String.format("key-%04d", i);
            MulticloudjUtils.upload(bc, key, "v".getBytes(StandardCharsets.UTF_8));
        }

        final Set<String> allCollected = new LinkedHashSet<>();
        int pageSize = 100;
        for (int start = 0; start < totalKeys; start += pageSize) {
            List<String> page = MulticloudjUtils.listKeysOrdered(bc, prefix, start, pageSize);
            for (String key : page) {
                boolean added = allCollected.add(key);
                assertTrue(added, "Duplicate key found in pagination: " + key);
            }
        }

        assertEquals(allCollected.size(), totalKeys, "Expected all keys to be paged through without gaps");
    }

    @Test
    public void listKeysOrdered_countMinusOne_returnsAll() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String prefix = "all-keys/";
        final int totalKeys = 50;

        for (int i = 0; i < totalKeys; i++) {
            String key = prefix + String.format("key-%03d", i);
            MulticloudjUtils.upload(bc, key, "x".getBytes(StandardCharsets.UTF_8));
        }

        List<String> result = MulticloudjUtils.listKeysOrdered(bc, prefix, 0, -1);
        assertEquals(result.size(), totalKeys, "count=-1 should return all keys");
    }

    // ─── getMatchingKeys tests ───────────────────────────────────────────────────

    @Test
    public void getMatchingKeys_minimalPrefixCover() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String namespaceKeyPrefix = "cantor-events/test-ns";

        // Window spanning 2 hours should produce hour-level prefixes
        long startMs = 1700000000000L; // 2023-11-14T22:13:20Z
        long endMs = startMs + TimeUnit.HOURS.toMillis(2); // +2 hours

        Set<String> prefixes = MulticloudjUtils.getMatchingKeys(bc, namespaceKeyPrefix, startMs, endMs);

        assertNotNull(prefixes);
        assertFalse(prefixes.isEmpty(), "Should produce at least one prefix");

        // All prefixes should start with the namespace key prefix
        for (String p : prefixes) {
            assertTrue(p.startsWith(namespaceKeyPrefix + "/"),
                    "Prefix should start with namespace key prefix: " + p);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void getMatchingKeys_rejectsStartAfterEnd() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        MulticloudjUtils.getMatchingKeys(bc, "cantor-events/ns", 2000L, 1000L);
    }

    // ─── deleteAllUnderPrefix / deleteBatched tests ──────────────────────────────

    @Test
    public void deleteBatched_removesAllKeys() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String prefix = "delete-test/";
        final int totalKeys = 250;

        for (int i = 0; i < totalKeys; i++) {
            String key = prefix + String.format("key-%03d", i);
            MulticloudjUtils.upload(bc, key, "data".getBytes(StandardCharsets.UTF_8));
        }

        assertEquals(MulticloudjUtils.countKeys(bc, prefix), totalKeys);

        MulticloudjUtils.deleteAllUnderPrefix(bc, prefix);

        assertEquals(MulticloudjUtils.countKeys(bc, prefix), 0, "All keys should be deleted");
    }

    // ─── wrapAsIOException tests ─────────────────────────────────────────────────

    @Test
    public void wrapAsIOException_includesOpAndNamespace() {
        RuntimeException cause = new RuntimeException("network error");
        IOException wrapped = MulticloudjUtils.wrapAsIOException("store", "my-namespace", cause);

        assertNotNull(wrapped);
        assertTrue(wrapped.getMessage().contains("store"), "Should include the operation name");
        assertTrue(wrapped.getMessage().contains("my-namespace"), "Should include the namespace");
        assertEquals(wrapped.getCause(), cause, "Should preserve the original cause");
    }

    // ─── doesObjectExist tests ───────────────────────────────────────────────────

    @Test
    public void doesObjectExist_returnsFalseForMissing() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        assertFalse(MulticloudjUtils.doesObjectExist(bc, "nonexistent-key"));
    }

    @Test
    public void doesObjectExist_returnsTrueAfterUpload() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        MulticloudjUtils.upload(bc, "exists-key", "hello".getBytes(StandardCharsets.UTF_8));
        assertTrue(MulticloudjUtils.doesObjectExist(bc, "exists-key"));
    }

    // ─── upload / download round-trip tests ──────────────────────────────────────

    @Test
    public void uploadAndDownload_roundTrip() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        String key = "round-trip/obj1";

        MulticloudjUtils.upload(bc, key, data);
        byte[] downloaded = MulticloudjUtils.download(bc, key);

        assertEquals(downloaded, data);
    }

    // ─── countKeys tests ─────────────────────────────────────────────────────────

    @Test
    public void countKeys_emptyPrefix() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        assertEquals(MulticloudjUtils.countKeys(bc, "empty-prefix/"), 0);
    }

    @Test
    public void countKeys_afterUploads() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        String prefix = "count-test/";

        MulticloudjUtils.upload(bc, prefix + "a", "1".getBytes(StandardCharsets.UTF_8));
        MulticloudjUtils.upload(bc, prefix + "b", "2".getBytes(StandardCharsets.UTF_8));
        MulticloudjUtils.upload(bc, prefix + "c", "3".getBytes(StandardCharsets.UTF_8));

        assertEquals(MulticloudjUtils.countKeys(bc, prefix), 3);
    }

    // ─── downloadRange tests ─────────────────────────────────────────────────────

    @Test
    public void downloadRange_returnsByteSubrange() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        byte[] data = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
        String key = "range-test/full";

        MulticloudjUtils.upload(bc, key, data);

        // The blob-inmemory provider may or may not natively support ranges. Either way,
        // MulticloudjUtils.downloadRange falls back to a slice and must return the correct
        // contiguous subrange [5, 9] = "56789".
        byte[] slice = MulticloudjUtils.downloadRange(bc, key, 5, 9);
        assertEquals(new String(slice, StandardCharsets.UTF_8), "56789");
    }

    @Test
    public void downloadRange_offsetZero_returnsFromStart() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        byte[] data = "abcdef".getBytes(StandardCharsets.UTF_8);
        MulticloudjUtils.upload(bc, "range-zero/k", data);

        byte[] slice = MulticloudjUtils.downloadRange(bc, "range-zero/k", 0, 2);
        assertEquals(new String(slice, StandardCharsets.UTF_8), "abc");
    }

    // ─── deleteBatched edge-case tests ───────────────────────────────────────────

    @Test
    public void deleteBatched_emptyCollection_isNoOp() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        // Should not throw for an empty collection.
        MulticloudjUtils.deleteBatched(bc, Collections.emptyList());
    }

    @Test
    public void deleteBatched_nullCollection_isNoOp() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        // Should not throw for null - documented no-op behavior.
        MulticloudjUtils.deleteBatched(bc, null);
    }

    // ─── matchesMetadata / matchesDimensions null-handling tests ─────────────────

    @Test
    public void matchesMetadata_nullQueryMatchesAnything() {
        assertTrue(MulticloudjUtils.matchesMetadata(Map.of("host", "web-01"), null));
    }

    @Test
    public void matchesMetadata_nullMetadataWithNonEmptyQuery_returnsFalse() {
        // metadata=null, query non-empty: a query expects a key that doesn't exist - should not match
        assertFalse(MulticloudjUtils.matchesMetadata(null, Map.of("host", "web-01")));
    }

    @Test
    public void matchesDimensions_nullQueryMatchesAnything() {
        assertTrue(MulticloudjUtils.matchesDimensions(Map.of("cpu", 0.5), null));
    }

    @Test
    public void matchesDimensions_nullDimensionsWithNonEmptyQuery_returnsFalse() {
        assertFalse(MulticloudjUtils.matchesDimensions(null, Map.of("cpu", "0.5")));
    }

    // ─── getMatchingKeys edge cases ──────────────────────────────────────────────

    @Test
    public void getMatchingKeys_startEqualsEnd_producesAtLeastOnePrefix() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        long ts = 1700000000000L;
        Set<String> prefixes = MulticloudjUtils.getMatchingKeys(bc, "cantor-events/ns", ts, ts);
        assertNotNull(prefixes);
        assertFalse(prefixes.isEmpty(), "start==end should still produce at least the boundary minute prefix");
    }

    // ─── max-download-size guard tests (F1: OOM prevention) ─────────────────────

    @Test
    public void download_throwsWhenBlobExceedsMaxDownloadSize() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String key = "oversized/blob";
        // Upload a 1 KiB blob.
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'x');
        MulticloudjUtils.upload(bc, key, data);

        final long original = MulticloudjUtils.getMaxDownloadSizeBytes();
        try {
            // Configure the max to a value smaller than the blob we just uploaded.
            MulticloudjUtils.setMaxDownloadSizeBytes(512);

            IOException ioe = expectThrows(IOException.class, () -> MulticloudjUtils.download(bc, key));
            assertTrue(ioe.getMessage() != null
                            && ioe.getMessage().toLowerCase().contains("exceeds maximum download size"),
                    "Exception message should mention exceeding max download size; got: " + ioe.getMessage());
        } finally {
            MulticloudjUtils.setMaxDownloadSizeBytes(original);
        }
    }

    @Test
    public void download_succeedsWhenBlobUnderMaxDownloadSize() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String key = "ok/blob";
        byte[] data = "small payload".getBytes(StandardCharsets.UTF_8);
        MulticloudjUtils.upload(bc, key, data);

        final long original = MulticloudjUtils.getMaxDownloadSizeBytes();
        try {
            MulticloudjUtils.setMaxDownloadSizeBytes(1024L * 1024L); // 1 MiB
            byte[] downloaded = MulticloudjUtils.download(bc, key);
            assertEquals(downloaded, data);
        } finally {
            MulticloudjUtils.setMaxDownloadSizeBytes(original);
        }
    }

    @Test
    public void setMaxDownloadSizeBytes_rejectsNonPositive() {
        final long original = MulticloudjUtils.getMaxDownloadSizeBytes();
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> MulticloudjUtils.setMaxDownloadSizeBytes(0));
            assertThrows(IllegalArgumentException.class,
                    () -> MulticloudjUtils.setMaxDownloadSizeBytes(-1));
        } finally {
            MulticloudjUtils.setMaxDownloadSizeBytes(original);
        }
    }

    // ─── deleteBatched large-batch test (verifies chunking) ──────────────────────

    @Test
    public void deleteBatched_largerThanBatchMax_deletesAll() throws IOException {
        final BucketClient bc = InMemoryBucketClients.fresh();
        final String prefix = "large-delete/";
        // 250 > DELETE_BATCH_MAX (100), forcing 3 batches
        final int total = 250;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String key = prefix + String.format("k-%04d", i);
            MulticloudjUtils.upload(bc, key, "x".getBytes(StandardCharsets.UTF_8));
            keys.add(key);
        }
        assertEquals(MulticloudjUtils.countKeys(bc, prefix), total);

        MulticloudjUtils.deleteBatched(bc, keys);

        assertEquals(MulticloudjUtils.countKeys(bc, prefix), 0,
                "All keys across multiple batches should be deleted");
    }
}

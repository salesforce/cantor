/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.cantor.multicloudj.support.InMemoryBucketClients;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.testng.Assert.*;

public class AbstractBaseMulticloudjNamespaceableTest {

    /**
     * Concrete subclass for testing the abstract base class.
     */
    private static class Probe extends AbstractBaseMulticloudjNamespaceable {
        Probe(BucketClient bucketClient) throws IOException {
            super(bucketClient, "test");
        }

        @Override
        protected String getObjectKeyPrefix(String namespace) {
            return "cantor-test/" + trim(namespace);
        }
    }

    @Test
    public void testConstructor_validatesBucketReachable() throws IOException {
        BucketClient bc = InMemoryBucketClients.fresh();
        Probe probe = new Probe(bc);
        assertNotNull(probe);
    }

    @Test
    public void testCreate_writesNamespaceMarkerAtExpectedKey() throws IOException {
        BucketClient bc = InMemoryBucketClients.fresh();
        Probe probe = new Probe(bc);

        probe.create("foo");

        String expectedMarkerKey = "cantor-test/" + AbstractBaseMulticloudjNamespaceable.trim("foo")
                + "/" + AbstractBaseMulticloudjNamespaceable.NAMESPACE_IDENTIFIER;
        assertTrue(MulticloudjUtils.doesObjectExist(bc, expectedMarkerKey),
                "Namespace marker should exist at: " + expectedMarkerKey);
    }

    @Test
    public void testCreate_isIdempotent() throws IOException {
        BucketClient bc = InMemoryBucketClients.fresh();
        Probe probe = new Probe(bc);

        probe.create("idempotent-ns");
        probe.create("idempotent-ns");

        String markerKey = "cantor-test/" + AbstractBaseMulticloudjNamespaceable.trim("idempotent-ns")
                + "/" + AbstractBaseMulticloudjNamespaceable.NAMESPACE_IDENTIFIER;
        assertTrue(MulticloudjUtils.doesObjectExist(bc, markerKey),
                "Marker should still exist after duplicate create");
    }

    @Test
    public void testDrop_removesAllKeysUnderPrefix() throws IOException {
        BucketClient bc = InMemoryBucketClients.fresh();
        Probe probe = new Probe(bc);

        probe.create("drop-ns");

        String prefix = "cantor-test/" + AbstractBaseMulticloudjNamespaceable.trim("drop-ns");
        for (int i = 0; i < 5; i++) {
            MulticloudjUtils.upload(bc, prefix + "/extra-key-" + i,
                    ("data-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // 5 extra keys + 1 namespace marker = 6 total
        assertEquals(MulticloudjUtils.countKeys(bc, prefix), 6);

        probe.drop("drop-ns");

        assertEquals(MulticloudjUtils.countKeys(bc, prefix), 0,
                "All keys including the namespace marker should be deleted");
    }

    @Test
    public void testTrim_lowercasesStripsAndAppendsHash() {
        // Verify the output matches the cantor-s3 AbstractBaseS3Namespaceable.trim() exactly
        String input = "My.NS/123";
        String expected = String.format("%s-%s",
                input.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase()
                        .substring(0, Math.min(64, input.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase().length())),
                Math.abs(input.hashCode()));
        String actual = AbstractBaseMulticloudjNamespaceable.trim(input);
        assertEquals(actual, expected,
                "trim() output must be byte-identical to AbstractBaseS3Namespaceable.trim()");
    }

    @Test
    public void testTrim_truncatesAt64Chars() {
        // Input longer than 64 chars after stripping
        String longInput = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01234567890";
        String trimmed = AbstractBaseMulticloudjNamespaceable.trim(longInput);

        // The format is: <cleanName up to 64 chars>-<abs(hashCode)>
        String cleanName = longInput.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase();
        String expectedPrefix = cleanName.substring(0, 64);
        assertTrue(trimmed.startsWith(expectedPrefix),
                "Trimmed name should start with the first 64 chars of the cleaned input");
        assertTrue(trimmed.contains("-" + Math.abs(longInput.hashCode())),
                "Trimmed name should end with the hash suffix");
    }

    @Test
    public void testConstructor_rejectsNullBucketClient() {
        assertThrows(IllegalArgumentException.class, () -> new Probe(null));
    }

    @Test
    public void testTrim_stripsDisallowedCharacters() {
        String trimmed = AbstractBaseMulticloudjNamespaceable.trim("Foo Bar!@#$%^&*()");
        // After the strip and lowercase, only "foobar" remains (whitespace and symbols stripped)
        // The format is "foobar-<abs(hashCode)>"
        assertTrue(trimmed.startsWith("foobar-"),
                "Disallowed characters should be stripped, got: " + trimmed);
    }

    @Test
    public void testTrim_emptyNamespaceProducesHashOnly() {
        String trimmed = AbstractBaseMulticloudjNamespaceable.trim("");
        // After strip, cleanName is "" (length 0). Substring(0, 0) = "". Format becomes "-<abs(0)>" = "-0"
        assertEquals(trimmed, "-0", "trim(\"\") should produce '-0'");
    }
}

/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import com.salesforce.cantor.Events;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

public abstract class AbstractBaseEventsTest extends AbstractBaseCantorTest {

    private final String namespace = UUID.randomUUID().toString();

    @BeforeMethod
    public void before() throws Exception {
        getEvents().create(this.namespace);
    }

    @AfterMethod
    public void after() throws Exception {
        getEvents().drop(this.namespace);
    }

    @Test
    public void testNamespaces() throws Exception {
        final Events events = getEvents();
        final List<String> namespaces = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            final String slashTestOnFives = (i % 5 == 0) ? "/" + UUID.randomUUID().toString() : "";
            final String namespace = String.format("%s%s", UUID.randomUUID().toString(), slashTestOnFives);
            namespaces.add(namespace);
            assertFalse(events.namespaces().contains(namespace));

            events.create(namespace);
            assertTrue(events.namespaces().contains(namespace));
        }

        for (final String namespace : namespaces) {
            events.drop(namespace);
            assertFalse(events.namespaces().contains(namespace));
        }
    }

    @Test
    public void testBadInput() throws Exception {
        final Events events = getEvents();
        assertThrows(IllegalArgumentException.class,
                () -> events.store(null, System.currentTimeMillis(), null, null));
        assertThrows(IllegalArgumentException.class, () -> events.store(namespace, -1, null, null));

        assertThrows(IOException.class, () -> events.store(UUID.randomUUID().toString(), 0, null, null));
    }

    @Test
    public void testMaxDimensionsCount() throws Exception {
        final Map<String, Double> dimensions = new HashMap<>();
        final Map<String, String> metadata = new HashMap<>();
        
        // create an event with 400 dimensions and 100 metadata
        for (int i = 0; i < 400; ++i) {
            dimensions.put(UUID.randomUUID().toString(), (double)i);
        }
        for (int i = 0; i < 100; ++i) {
            metadata.put(UUID.randomUUID().toString(), String.valueOf(i));
        }
        getEvents().store(this.namespace, 0, metadata, dimensions);
        final List<Events.Event> results = getEvents().get(this.namespace, 0, 1);
        final Map<String, Double> returnedDimensions = results.get(0).getDimensions();
        final Map<String, String> returnedMetadata = results.get(0).getMetadata();

        // check that all dimensions and metadata are returned; ordering is not important
        assertEquals(dimensions.keySet(), returnedDimensions.keySet());
        assertEquals(metadata.keySet(), returnedMetadata.keySet());
        for (final Map.Entry<String, Double> entry : returnedDimensions.entrySet()) {
            assertEquals(entry.getValue(), dimensions.get(entry.getKey()));
        }
        for (final Map.Entry<String, String> entry : returnedMetadata.entrySet()) {
            assertEquals(entry.getValue(), metadata.get(entry.getKey()));
        }

        // one more and we should get illegal argument exception
        dimensions.put(UUID.randomUUID().toString(), 0.0);
        metadata.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> getEvents().store(this.namespace, 0, metadata, dimensions));
    }

    @Test
    public void testStoreOneEvent() throws Exception {
        final Events events = getEvents();
        final long timestamp = System.currentTimeMillis();
        final Map<String, String> metadata = getRandomMetadata(100);
        final Map<String, Double> dimensions = getRandomDimensions(400);
        final byte[] payload = getRandomPayload(8 * 1024 * 1024);
        events.store(this.namespace, timestamp, metadata, dimensions, payload);
        final List<Events.Event> results = events.get(
                this.namespace,
                timestamp, timestamp + 1,
                true
        );
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getTimestampMillis(), timestamp);
        assertEquals(results.get(0).getPayload(), payload);
        final Map<String, String> returnedMetadata = results.get(0).getMetadata();
        logger.info("metadata: {}", metadata);
        logger.info("returned metadata: {}", returnedMetadata);
        for (final Map.Entry<String, String> entry : metadata.entrySet()) {
            assertEquals(returnedMetadata.get(entry.getKey()), entry.getValue());
        }
        final Map<String, Double> returnedDimensions = results.get(0).getDimensions();
        for (final Map.Entry<String, Double> entry : dimensions.entrySet()) {
            assertEquals(returnedDimensions.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testMetadataQueryRegex() throws Exception {
        final Events events = getEvents();
        final long timestamp = System.currentTimeMillis();
        final Map<String, String> metadata = getRandomMetadata(10);
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10, 100); ++i) {
            events.store(this.namespace, timestamp + ThreadLocalRandom.current().nextInt(-100, 100), metadata, null);
        }

        final int matchCount = ThreadLocalRandom.current().nextInt(3, 10);
        for (int i = 0; i < matchCount; ++i) {
            metadata.put("metadata-key-" + i,
                    // "<random>--pattern--[3-digit-number]--<random>"
                    UUID.randomUUID().toString()
                    + "--pattern--" + ThreadLocalRandom.current().nextInt(100, 999) + "--"
                    + UUID.randomUUID().toString());
        }
        for (int i = 0; i < matchCount; ++i) {
            events.store(this.namespace, timestamp + ThreadLocalRandom.current().nextInt(-100, 100), metadata, null);
        }

        final Map<String, String> metadataQuery = new HashMap<>();
        for (int i = 0; i < matchCount; ++i) {
            metadataQuery.put("metadata-key-" + i, "~*--pattern--*--*");
            assertEquals(events.get(this.namespace, timestamp - 100, timestamp + 101, metadataQuery, null).size(), matchCount);
        }

        // should not find anything now, because there is no 'metadata-key-<match-count>'
        metadataQuery.put("metadata-key-" + matchCount, "~*-pattern-*");
        assertEquals(events.get(this.namespace, timestamp - 100, timestamp + 101, metadataQuery, null).size(), 0);
    }

    @Test
    public void testMetadataQueryEscape() throws Exception {
        final Events events = getEvents();
        final long timestamp = System.currentTimeMillis();

        final Map<String, String> metadataMatched = new HashMap<>();
        final Map<String, String> metadataNotMatched = new HashMap<>();
        final String escapePattern = "%%%%%%_____"; // % and _ should be correctly escaped in SQL queries
        String patternNotMatched;
        final int matchCount = ThreadLocalRandom.current().nextInt(3, 10);
        for (int i = 0; i < matchCount; ++i) {
            patternNotMatched = UUID.randomUUID().toString().substring(escapePattern.length());
            metadataMatched.put("metadata-key-" + i, UUID.randomUUID().toString() +
                    escapePattern + UUID.randomUUID().toString());
            metadataNotMatched.put("metadata-key-" + i, UUID.randomUUID().toString() +
                    patternNotMatched + UUID.randomUUID().toString());
        }
        for (int i = 0; i < matchCount; ++i) {
            events.store(this.namespace, timestamp + ThreadLocalRandom.current().nextInt(-100, 100),
                    metadataMatched, null);
            events.store(this.namespace, timestamp + ThreadLocalRandom.current().nextInt(101, 300),
                    metadataNotMatched, null);
        }

        // Events with metadata from 'metadataMatched' should be matched; those with metadata from 'metadataNotMatched'
        // will not be matched.
        final Map<String, String> metadataQuery = new HashMap<>();
        for (int i = 0; i < matchCount; ++i) {
            metadataQuery.put("metadata-key-" + i, "~*" + escapePattern + "*");
            assertEquals(events.get(this.namespace, timestamp - 100, timestamp + 300, metadataQuery, null).size(), matchCount);
        }
    }

    @Test
    public void testStore3kRandomEvents() throws Exception {
        final Events events = getEvents();
        final List<Events.Event> storedEvents = new ArrayList<>();
        final int metadataCount = ThreadLocalRandom.current().nextInt(10, 30);
        final int dimensionCount = ThreadLocalRandom.current().nextInt(10, 30);
        long timestamp = System.currentTimeMillis();
        final long firstTimestamp = timestamp;
        Events.Event firstEvent = null;
        Events.Event lastEvent = null;
        for (int i = 0; i < 3_000; ++i) {
            final Map<String, String> metadata = getRandomMetadata(metadataCount);
            final Map<String, Double> dimensions = getRandomDimensions(dimensionCount);
            final byte[] payload = getRandomPayload(1024);
            timestamp += 1;
            lastEvent = new Events.Event(timestamp, metadata, dimensions, payload);
            if (firstEvent == null) {
                firstEvent = lastEvent;
            }
            storedEvents.add(lastEvent);
        }
        logger.info("calling events.store(batch)");
        final long startTimestamp = System.currentTimeMillis();
        events.store(this.namespace, storedEvents);
        final long afterStoreTimestamp = System.currentTimeMillis();
        logger.info("took {}ms to store 3k events", afterStoreTimestamp - startTimestamp);
        final List<Events.Event> results = getEvents().get(this.namespace, firstTimestamp, timestamp + 1, true);
        logger.info("took {}ms to get 3k events", System.currentTimeMillis() - afterStoreTimestamp);
        assertEquals(results.size(), storedEvents.size());
        for (int i = 0; i < storedEvents.size(); ++i) {
            assertEquals(results.get(i).getTimestampMillis(), storedEvents.get(i).getTimestampMillis());
            assertEquals(results.get(i).getPayload(), storedEvents.get(i).getPayload());
            for (final Map.Entry<String, String> entry : storedEvents.get(i).getMetadata().entrySet()) {
                assertEquals(results.get(i).getMetadata().get(entry.getKey()), entry.getValue());
            }
            for (final Map.Entry<String, Double> entry : storedEvents.get(i).getDimensions().entrySet()) {
                assertEquals(results.get(i).getDimensions().get(entry.getKey()), entry.getValue());
            }
        }

        // check dimension query between two values
        final double start = ThreadLocalRandom.current().nextDouble();
        final double end = start + ThreadLocalRandom.current().nextDouble();
        final String dimension = (String) (storedEvents.get(0).getDimensions().keySet().toArray()[0]);
        final Map<String, String> dimensionBetweenQuery = new HashMap<>();
        dimensionBetweenQuery.put(dimension, start + ".." + end);
        final List<Events.Event> resultsBetween = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                Collections.emptyMap(),
                dimensionBetweenQuery
        );
        for (final Events.Event e : resultsBetween) {
            assertTrue(e.getDimensions().get(dimension) > start);
            assertTrue(e.getDimensions().get(dimension) < end);
        }

        // check dimension query greater than a value
        final Map<String, String> dimensionGreaterQuery = new HashMap<>();
        dimensionGreaterQuery.put(dimension, ">" + start);
        final List<Events.Event> resultsGreater = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                Collections.emptyMap(),
                dimensionGreaterQuery
        );
        for (final Events.Event e : resultsGreater) {
            assertTrue(e.getDimensions().get(dimension) > start);
        }

        // check dimension query greater than or equal a value
        final Map<String, String> dimensionGreaterEqualQuery = new HashMap<>();
        dimensionGreaterEqualQuery.put(dimension, ">=" + start);
        final List<Events.Event> resultsGreaterEqual = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                Collections.emptyMap(),
                dimensionGreaterEqualQuery
        );
        for (final Events.Event e : resultsGreaterEqual) {
            assertTrue(e.getDimensions().get(dimension) >= start);
        }

        // check dimension query less than a value
        final Map<String, String> dimensionLessQuery = new HashMap<>();
        dimensionLessQuery.put(dimension, "<" + end);
        final List<Events.Event> resultsLess = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                Collections.emptyMap(),
                dimensionLessQuery
        );
        for (final Events.Event e : resultsLess) {
            assertTrue(e.getDimensions().get(dimension) < end);
        }

        // check dimension query less than or equal a value
        final Map<String, String> dimensionLessEqualQuery = new HashMap<>();
        dimensionLessEqualQuery.put(dimension, "<=" + end);
        final List<Events.Event> resultsLessEqual = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                Collections.emptyMap(),
                dimensionLessEqualQuery
        );
        for (final Events.Event e : resultsLessEqual) {
            assertTrue(e.getDimensions().get(dimension) <= end);
        }

        final String metadata = (String) (storedEvents.get(0).getMetadata().keySet().toArray()[0]);
        // check metadata query like a value with prefix
        final Map<String, String> metadataLikePrefixQuery = new HashMap<>();
        final String prefix = "a";
        metadataLikePrefixQuery.put(metadata, "~" + prefix + "*");
        final List<Events.Event> resultsLikePrefix = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                metadataLikePrefixQuery,
                null
        );
        for (final Events.Event e : resultsLikePrefix) {
            assertTrue(e.getMetadata().get(metadata).startsWith(prefix));
        }
        final Map<String, String> metadataNotLikePrefixQuery = new HashMap<>();
        metadataNotLikePrefixQuery.put(metadata, "!~" + prefix + "*");
        final List<Events.Event> resultsNotLikePrefix = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                metadataNotLikePrefixQuery,
                null
        );
        for (final Events.Event e : resultsNotLikePrefix) {
            assertFalse(e.getMetadata().get(metadata).startsWith(prefix));
        }

        // check metadata query like a value with postfix
        final Map<String, String> metadataLikePostfixQuery = new HashMap<>();
        final String postfix = "a";
        metadataLikePostfixQuery.put(metadata, "~*" + postfix);
        final List<Events.Event> resultsLikePostfix = getEvents().get(
                this.namespace,
                startTimestamp, timestamp + 1,
                metadataLikePostfixQuery,
                null
        );
        for (final Events.Event e : resultsLikePostfix) {
            assertTrue(e.getMetadata().get(metadata).endsWith(postfix));
        }
    }

    @Test
    public void testCreateDrop() throws Exception {
        final Events events = getEvents();
        final String randomNamespace = UUID.randomUUID().toString();
        final long timestamp = System.currentTimeMillis();
        // fails to store when namespace is not created yet
        assertThrows(IOException.class,
                () -> events.store(randomNamespace, timestamp, null, null)
        );
        // create the namespace, and store must not fail anymore
        events.create(randomNamespace);
        events.store(randomNamespace, timestamp, null, null);
        final Events.Event returnedEvent = events.get(randomNamespace, timestamp, timestamp + 1, null, null).get(0);
        assertEquals(timestamp, returnedEvent.getTimestampMillis());
        // drop the namespace and store must fail again
        events.drop(randomNamespace);
        assertThrows(IOException.class,
                () -> events.store(randomNamespace, timestamp, null, null)
        );
    }

    @Test
    public void testMetadata() throws Exception {
        final Events events = getEvents();
        long startTimestampMillis = System.currentTimeMillis();

        final Set<String> metadataValues = new HashSet<>();

        final int total = ThreadLocalRandom.current().nextInt(100, 1000);
        logger.info("storing {} random events", total);
        for (int i = 0; i < total; ++i) {
            final String metadataValue = UUID.randomUUID().toString();
            metadataValues.add(metadataValue);

            final Map<String, String> metadata = new HashMap<>();
            metadata.put("unused-metadata-key", UUID.randomUUID().toString());
            metadata.put("test-metadata-key", metadataValue);
            events.store(this.namespace, startTimestampMillis + i, metadata, null);
        }

        final Set<String> metadataResults = events.metadata(
                this.namespace,
                "test-metadata-key",
                startTimestampMillis - 1,
                startTimestampMillis + total + 1,
                null,
                null
        );
        assertEquals(metadataResults.size(), metadataValues.size());
        for (final String entry : metadataValues) {
            assertTrue(metadataResults.contains(entry));
        }
    }

    @Test
    public void testDimension() throws Exception {
        final Events events = getEvents();
        final String testDimensionKey = "test-dimension-key";
        final double testDimensionValue = 123D;
        final long startTimestampMillis = System.currentTimeMillis();
        final int total = ThreadLocalRandom.current().nextInt(100, 1000);
        logger.info("storing {} random events", total);
        for (int i = 0; i < total; ++i) {
            final Map<String, Double> dimensions = getRandomDimensions(5);
            // only add a specific dimension key to half of the events to be stored
            if ((i + 1) % 2 == 0) dimensions.put(testDimensionKey, testDimensionValue + i);
            events.store(this.namespace, startTimestampMillis + i, getRandomMetadata(5), dimensions);
        }
        final List<Events.Event> eventsRetrieved = events.dimension(
            this.namespace,
            testDimensionKey,
            startTimestampMillis,
            startTimestampMillis + total,
            Collections.emptyMap(),
            Collections.singletonMap(testDimensionKey, ">=" + testDimensionValue)
        );
        assertEquals(eventsRetrieved.size(), total / 2);
        for (final Events.Event e : eventsRetrieved) {
            assertTrue(e.getTimestampMillis() >= startTimestampMillis && e.getTimestampMillis() <= startTimestampMillis + total);
            assertEquals(e.getMetadata().size(), 0);
            assertEquals(e.getDimensions().size(), 1);
            assertTrue(e.getDimensions().get(testDimensionKey) >= testDimensionValue);
        }
    }

    private void checkSum(final Map<Long, Double> sumResults, final double sum) {
        logger.info("{}", sumResults);
        double returnedSum = 0.0;
        for (final Map.Entry<Long, Double> entry : sumResults.entrySet()) {
            returnedSum += entry.getValue();
        }
        // can't rely on operations on double values with high precision,
        // so just check that the returned sum is close enough to expected
        assertTrue(Math.abs(sum - returnedSum) < 0.1);
    }

    private Map<String, Double> getRandomDimensions(final int count) {
        final Map<String, Double> dimensions = new HashMap<>();
        for (int i = 0; i < count; ++i) {
            dimensions.put("random-keys-like-!@#$%^&*()_+=-/?,>,<`-are-all-accepted; " +
                    "even really really unnecessarily long keys like this is acceptable!..." + i, ThreadLocalRandom.current().nextDouble());
        }
        return dimensions;
    }

    private Map<String, String> getRandomMetadata(final int count) {
        final Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < count; ++i) {
            metadata.put("random-keys-like-!@#$%^&*()_+=-/?,>,<`-are-all-accepted; " +
                    "even really really unnecessarily long keys like this is acceptable!..." + i, UUID.randomUUID().toString());
        }
        return metadata;
    }

    private byte[] getRandomPayload(final int size) {
        final byte[] buffer = new byte[size];
        new Random().nextBytes(buffer);
        return buffer;
    }

    private Events getEvents() throws IOException {
        return getCantor().events();
    }

}

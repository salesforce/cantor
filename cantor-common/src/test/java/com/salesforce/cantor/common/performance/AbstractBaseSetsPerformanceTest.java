/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common.performance;

import com.salesforce.cantor.Sets;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public abstract class AbstractBaseSetsPerformanceTest extends AbstractBaseCantorPerformanceTest {
    private final String namespace = UUID.randomUUID().toString();

    @BeforeMethod
    public void before() throws Exception {
        getSets().create(this.namespace);
    }

    @AfterMethod
    public void after() throws Exception {
        getSets().drop(this.namespace);
    }

    @Test
    public void testNamespaces() throws Exception {
        final Sets sets = getSets();
        final List<String> namespaces = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            final String namespace = UUID.randomUUID().toString();
            namespaces.add(namespace);
            assertFalse(sets.namespaces().contains(namespace));

            sets.create(namespace);
            assertTrue(sets.namespaces().contains(namespace));
        }

        for (final String namespace : namespaces) {
            sets.drop(namespace);
            assertFalse(sets.namespaces().contains(namespace));
        }
    }
    @Test
    public void testMinMax() throws IOException {
        final Sets sets = getSets();

        final String setName = UUID.randomUUID().toString();
        sets.add(this.namespace, setName, "min", Long.MIN_VALUE);
        sets.add(this.namespace, setName, "zero", 0);
        sets.add(this.namespace, setName, "max", Long.MAX_VALUE);

        assertEquals(sets.get(this.namespace, setName).size(), 3);
        assertEquals(sets.weight(this.namespace, setName, "min").longValue(), Long.MIN_VALUE);
        assertEquals(sets.weight(this.namespace, setName, "zero").longValue(), 0);
        assertEquals(sets.weight(this.namespace, setName, "max").longValue(), Long.MAX_VALUE);
    }

//    @Test
    public void testConcurrency() throws Exception {
        final Sets sets = getSets();
        final String setName = UUID.randomUUID().toString();
        final ExecutorService executor = Executors.newCachedThreadPool();
        final int entriesCount = 1000;
        final int producersCount = 10;
        final int consumersCount = 10;
        final AtomicInteger producedCount = new AtomicInteger(0);
        final AtomicInteger consumedCount = new AtomicInteger(0);
        // producers
        for (int i = 0; i < producersCount; ++i) {
            executor.submit(() -> {
                for (int j = 0; j < entriesCount / producersCount; ++j) {
                    try {
                        sets.add(this.namespace, setName, UUID.randomUUID().toString(), ThreadLocalRandom.current().nextLong());
                        producedCount.incrementAndGet();
                        logger.info("finished producing {} entries.", producedCount.get());
                    } catch (IOException e) {
                        logger.warn("exception caught", e);
                        j--;
                    }
                }
            });
        }
        // consumers
        for (int i = 0; i < consumersCount; ++i) {
            executor.submit(() -> {
                while (true) {
                    try {
                        if (sets.size(this.namespace, setName) == 0) {
                            break;
                        }
                        final Map<String, Long> entries = sets.pop(this.namespace, setName, 0, 10);
                        consumedCount.addAndGet(entries.size());
                        logger.info("consumed {} entries", consumedCount.get());
                    } catch (IOException e) {
                        logger.warn("exception caught", e);
                    }
                    logger.info("finished consuming {} entries.", consumedCount.get());
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        assertEquals(producedCount.get(), entriesCount, "total number of entries produced is incorrect");
        assertEquals(consumedCount.get(), producedCount.get(), "produced entries != consumed entries");
    }

    @Test
    public void testAdd() throws IOException {
        final Sets sets = getSets();

        // test bad add() input
        assertThrows(IllegalArgumentException.class, () -> sets.add(namespace, null, "e", 0L));
        assertThrows(IllegalArgumentException.class, () -> sets.add(namespace, "", "e", 0L));
        assertThrows(IllegalArgumentException.class, () -> sets.add(namespace, "k", null, 0L));
        assertThrows(IllegalArgumentException.class, () -> sets.add(namespace, "k", "", 0L));

        assertThrows(IOException.class, () -> sets.add(UUID.randomUUID().toString(), "foo", "bar"));

        // test single add()
        final String singleAddSetKey = UUID.randomUUID().toString();
        final int singleAddCount = getCount(100, 200);
        final List<String> singleAddKeys = addRandoms(namespace, sets, singleAddSetKey, singleAddCount);

        final Map<String, Long> singleAddKeysWeights = sets.get(namespace, singleAddSetKey, 0, singleAddCount, 0, -1, true);
        assertEquals(singleAddKeysWeights.size(), singleAddCount, "get() should return all the entries added");
        for (int i = 0; i < singleAddKeys.size(); i++) {
            final String keyI = singleAddKeys.get(i);
            final Long valI = singleAddKeysWeights.get(keyI);
            assertNotNull(valI, "get() didn't return weight for key=" + keyI);
            assertEquals(valI.longValue(), i, "entries were added with weight in order, should match");
        }

        // test batch add()
        final String batchAddSetKey = UUID.randomUUID().toString();
        final int batchAddCount = getCount(100, 500);
        final long minWeight = 1_000, maxWeight = 2_000;

        final Map<String, Long> batchAddEntries = getRandoms(batchAddCount).stream()
                .collect(Collectors.toMap(Function.identity(), (s) -> ThreadLocalRandom.current().nextLong(minWeight, maxWeight)));
        sets.add(namespace, batchAddSetKey, batchAddEntries);

        final Map<String, Long> batchAddKeysWeights = sets.get(namespace, batchAddSetKey, minWeight, maxWeight, 0, -1, true);
        assertEquals(batchAddKeysWeights.size(), batchAddEntries.size(), "get() didn't return all entries added");
        for (Map.Entry<String, Long> entry : batchAddKeysWeights.entrySet()) {
            assertTrue(batchAddEntries.containsKey(entry.getKey()), "get() didn't return weight for key=" + entry.getKey());
            assertEquals(entry.getValue(), batchAddEntries.get(entry.getKey()), "get() didn't return correct weight for key=" + entry.getKey());
        }

        // test noop add()
        final String noopAddKey = UUID.randomUUID().toString();
        addRandoms(namespace, sets, noopAddKey, getCount(1, 100));
        final Map<String, Long> before = sets.get(namespace, noopAddKey, Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, true);
        sets.add(namespace, noopAddKey, Collections.emptyMap());
        final Map<String, Long> after = sets.get(namespace, noopAddKey, Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, true);
        assertEqualsDeep(after, before, "noop add shouldn't have added anything");
    }

    @Test
    public void testGet() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();
        final int totalKeysCount = getCount(100, 300);
        final List<String> randomKeys = addRandoms(namespace, sets, setKey, totalKeysCount);

        // test bad get() input
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, null, 0, 1, 0, -1, true));  // null set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "", 0, 1, 0, -1, true));  // empty set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "s", 0, -1, 0, -1, true)); // max < min should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "s", 0, 1, 0, -2, true)); // -2 count should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "s", 0, 1, 2, -1, true)); // -1 count non-zero start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "s", 0, 1, -1, -1, true)); // negative start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.get(namespace, "s", 0, 1, -1, 10, true)); // negative start should throw

        // test ascending vs descending get()
        final Map<String, Long> getAscending = sets.get(namespace, setKey, 0, totalKeysCount, 0, -1, true);
        final Map<String, Long> getDescending = sets.get(namespace, setKey, 0, totalKeysCount, 0, -1, false);
        assertEquals(getAscending.size(), getDescending.size(), "ascending vs descending shouldn't change result count");

        final ListIterator<String> ascendingKeys = new ArrayList<>(getAscending.keySet()).listIterator();
        final ListIterator<String> descendingKeys = new ArrayList<>(getDescending.keySet()).listIterator(getDescending.size());

        while (ascendingKeys.hasNext()) {
            assertTrue(descendingKeys.hasPrevious());
            assertEquals(ascendingKeys.next(), descendingKeys.previous(), "entries should be in reverse for ascending vs descending");
        }

        final Map<String, Long> zeroTo10 = sets.get(namespace, setKey, 0, totalKeysCount, 0, 10, true);
        final Map<String, Long> zeroTo20 = sets.get(namespace, setKey, 0, totalKeysCount, 0, 20, true);
        final Map<String, Long> fiftyToEnd = sets.get(namespace, setKey, 0, totalKeysCount, 50, totalKeysCount - 50, true);

        assertEquals(zeroTo10.size(), 10, "10 count was specified, size should be 10");
        assertEquals(zeroTo20.size(), 20, "10 count was specified, size should be 10");
        assertEquals(fiftyToEnd.size(), totalKeysCount - 50, "50 to end was specified, size should be total - 50");

        assertEquals(zeroTo10.keySet().iterator().next(), sets.first(namespace, setKey), "first key returned should be first in set");
        assertEquals(zeroTo20.keySet().iterator().next(), sets.first(namespace, setKey), "first key returned should be first in set");
        assertEquals(getLast(fiftyToEnd.keySet()), sets.last(namespace, setKey), "last key returned should be last in set");
        assertTrue(zeroTo10.keySet().stream().allMatch(zeroTo20::containsKey), "everything in 0-10 should also be in 0-20");
        assertTrue(fiftyToEnd.keySet().stream().noneMatch(zeroTo20::containsKey), "nothing in 0-20 should be in 50-100");

        // test min/max
        final Map<String, Long> min25 = sets.get(namespace, setKey, 25, totalKeysCount, 0, -1, true);
        assertTrue(min25.values().stream().allMatch(w -> w >= 25), "all weights should be 25 or over");

        final Map<String, Long> max24 = sets.get(namespace, setKey, 0, 24, 0, -1, true);
        assertTrue(max24.values().stream().allMatch(w -> w < 25), "all weights should be under 25");

        final Map<String, Long> twenties = sets.get(namespace, setKey, 20, 29, 0, -1, true);
        assertTrue(twenties.values().stream().allMatch(w -> w >= 20 && w <= 29), "all weights should be in the twenties");
    }

    @Test
    public void testUnion() throws IOException {
        final Sets sets = getSets();

        final List<String> setNames = new ArrayList<>();
        final Set<String> allEntries = new HashSet<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 100); ++i) {
            final String setName = UUID.randomUUID().toString();
            setNames.add(setName);
            for (int j = 0; j < ThreadLocalRandom.current().nextInt(0, 1000); ++j) {
                final String entry = j % 2 == 0 ? "common-entry" : UUID.randomUUID().toString();
                allEntries.add(entry);
                sets.add(namespace, setName, entry);
            }
        }

        final Collection<String> returnedEntries = sets.union(namespace, setNames).keySet();
        logger.info("all entries size: {} returned entries size for union: {}", allEntries.size(), returnedEntries.size());
        assertEquals(returnedEntries.size(), allEntries.size());
    }

//    @Test
    public void testIntersect() throws IOException {
        final Sets sets = getSets();

        final List<String> setNames = new ArrayList<>();
        final Set<String> allEntries = new HashSet<>();
        int weight = 0;
        final String commonEntry = UUID.randomUUID().toString();
        allEntries.add(commonEntry);
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 100); ++i) {
            final String setName = UUID.randomUUID().toString();
            setNames.add(setName);
            sets.add(namespace, setName, commonEntry);
            for (int j = 0; j < ThreadLocalRandom.current().nextInt(2, 1000); ++j) {
                final String entry = UUID.randomUUID().toString();
                allEntries.add(entry);
                sets.add(namespace, setName, entry, weight);
            }
        }

        final Map<String, Long> returnedEntries = sets.intersect(namespace, setNames);
        logger.info("all entries size: {} returned entries size for intersect: {}", allEntries.size(), returnedEntries.size());
        logger.info("returned entries are: {}", returnedEntries.size());
        // there is only one entry common in all sets
        assertEquals(returnedEntries.keySet().size(), 1);
    }

    @Test
    public void testPop() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();
        final int totalKeysCount = getCount(100, 300);
        addRandoms(namespace, sets, setKey, totalKeysCount);

        // test bad pop() input
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, null, 0, 1, 0, -1, true));  // null set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "", 0, 1, 0, -1, true));  // empty set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "s", 0, -1, 0, -1, true)); // max < min should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "s", 0, 1, 0, -2, true)); // -2 count should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "s", 0, 1, 2, -1, true)); // -1 count non-zero start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "s", 0, 1, -1, -1, true)); // negative start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.pop(namespace, "s", 0, 1, -1, 10, true)); // negative start should throw

        assertEquals(sets.size(namespace, setKey), totalKeysCount);
        // test ascending vs descending pop()
        final Map<String, Long> popAscending = sets.pop(namespace, setKey, 0, totalKeysCount, 0, -1, true);
        // all entries must have been removed
        assertEquals(sets.size(namespace, setKey), 0);

        final List<String> randomKeys2 = addRandoms(namespace, sets, setKey, totalKeysCount);

        assertEquals(sets.size(namespace, setKey), totalKeysCount);
        // test ascending vs descending pop()
        final Map<String, Long> popDescending = sets.pop(namespace, setKey, 0, totalKeysCount, 0, -1, false);
        assertEquals(sets.size(namespace, setKey), 0);

        assertEquals(popAscending.size(), popDescending.size(), "ascending vs descending shouldn't change result count");

        addRandoms(namespace, sets, setKey, totalKeysCount);
        final Map<String, Long> zeroTo10 = sets.pop(namespace, setKey, 0, totalKeysCount, 0, 10, true);
        final Map<String, Long> zeroTo20 = sets.pop(namespace, setKey, 0, totalKeysCount, 0, 20, true);
        final Map<String, Long> thirtyToEnd = sets.pop(namespace, setKey, 0, totalKeysCount, 0, totalKeysCount - 30, true);
        // nothing should be left in the set
        assertEquals(sets.size(namespace, setKey), 0);

        assertEquals(zeroTo10.size(), 10, "10 count was specified, size should be 10");
        assertEquals(zeroTo20.size(), 20, "10 count was specified, size should be 10");
        assertEquals(thirtyToEnd.size(), totalKeysCount - 30, "30 to end was specified, size should be total - 50");

        addRandoms(namespace, sets, setKey, totalKeysCount);
        // test min/max
        final Map<String, Long> min25 = sets.pop(namespace, setKey, 25, totalKeysCount, 0, -1, true);
        assertTrue(min25.values().stream().allMatch(w -> w >= 25), "all weights should be 25 or over");

        addRandoms(namespace, sets, setKey, totalKeysCount);
        final Map<String, Long> max24 = sets.pop(namespace, setKey, 0, 24, 0, -1, true);
        assertTrue(max24.values().stream().allMatch(w -> w < 25), "all weights should be under 25");

        final Map<String, Long> twenties = sets.pop(namespace, setKey, 20, 29, 0, -1, true);
        assertTrue(twenties.values().stream().allMatch(w -> w >= 20 && w <= 29), "all weights should be in the twenties");
    }

    @Test
    public void testKeys() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();
        final int count = getExactCount(51, 100);
        final List<String> randomKeys = addRandoms(namespace, sets, setKey, count);

        // test bad entries() input
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, null, 0, 1, 0, -1, true));  // null set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "", 0, 1, 0, -1, true));  // empty set should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "s", 0, -1, 0, -1, true)); // max < min should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "s", 0, 1, 0, -2, true)); // -2 count should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "s", 0, 1, 2, -1, true)); // -1 count non-zero start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "s", 0, 1, -1, -1, true)); // negative start should throw
        assertThrows(IllegalArgumentException.class, () -> sets.entries(namespace, "s", 0, 1, -1, 10, true)); // negative start should throw

        // test ascending vs descending entries()
        final List<String> keysAscending = new ArrayList<>(sets.entries(namespace, setKey, 0, count, 0, -1, true));
        final List<String> keysDescending = new ArrayList<>(sets.entries(namespace, setKey, 0, count, 0, -1, false));
        assertEquals(keysAscending.size(), keysDescending.size(), "ascending vs descending shouldn't change result count");

        final ListIterator<String> ascendingKeys = keysAscending.listIterator();
        final ListIterator<String> descendingKeys = keysDescending.listIterator(keysDescending.size());

        while (ascendingKeys.hasNext()) {
            assertTrue(descendingKeys.hasPrevious());
            assertEquals(ascendingKeys.next(), descendingKeys.previous(), "entries should be in reverse for ascending vs descending");
        }

        final List<String> zeroTo10 = new ArrayList<>(sets.entries(namespace, setKey, 0, count, 0, 10, true));
        final List<String> zeroTo20 = new ArrayList<>(sets.entries(namespace, setKey, 0, count, 0, 20, true));
        final List<String> fiftyToEnd = new ArrayList<>(sets.entries(namespace, setKey, 0, count, 50, count - 50, true));

        assertEquals(zeroTo10.size(), 10, "10 count was specified, size should be 10");
        assertEquals(zeroTo20.size(), 20, "10 count was specified, size should be 10");
        assertEquals(fiftyToEnd.size(), count - 50, "50 count was specified, size should be count - 50");

        assertEquals(zeroTo10.get(0), sets.first(namespace, setKey), "first key returned should be first in set");
        assertEquals(zeroTo20.get(0), sets.first(namespace, setKey), "first key returned should be first in set");
        assertEquals(fiftyToEnd.get(fiftyToEnd.size() - 1), sets.last(namespace, setKey), "last key returned should be last in set");
        assertTrue(zeroTo10.stream().allMatch(zeroTo20::contains), "everything in 0-10 should also be in 0-20");
        assertTrue(fiftyToEnd.stream().noneMatch(zeroTo20::contains), "nothing in 0-20 should be in 50-100");

        // test min/max
        final List<String> min25 = new ArrayList<>(sets.entries(namespace, setKey, 25, count, 0, -1, true));
        assertTrue(min25.stream().map(randomKeys::indexOf).allMatch(w -> w >= 25), "all weights should be 25 or over");

        final List<String> max24 = new ArrayList<>(sets.entries(namespace, setKey, 0, 24, 0, -1, true));
        assertTrue(max24.stream().map(randomKeys::indexOf).allMatch(w -> w < 25), "all weights should be under 25");

        final List<String> twenties = new ArrayList<>(sets.entries(namespace, setKey, 20, 29, 0, -1, true));
        assertTrue(twenties.stream().map(randomKeys::indexOf).allMatch(w -> w >= 20 && w <= 29), "all weights should be in the twenties");
    }

    @Test
    public void testDelete() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();
        final int count = getCount(50, 100);

        // test delete specific entries
        sets.add(namespace, setKey, "foobar", 123L);
        sets.add(namespace, setKey, "fizzbuzz", 123L);
        assertTrue(sets.delete(namespace, setKey, "foobar"));

        assertEquals(sets.size(namespace, setKey), 1);
        assertTrue(sets.delete(namespace, setKey, "fizzbuzz"));
        assertFalse(sets.delete(namespace, setKey, "does-not-exist"));

        // test delete() between for all
        addRandoms(namespace, sets, setKey, count);
        assertEquals(sets.size(namespace, setKey), count);
        sets.delete(namespace, setKey, 0, count);
        assertEquals(sets.size(namespace, setKey), 0);

        // test delete() between for range outside of added
        final List<String> randomKeys = addRandoms(namespace, sets, setKey, count);
        assertEquals(sets.size(namespace, setKey), count);
        sets.delete(namespace, setKey, count + 1, Long.MAX_VALUE);
        assertEquals(sets.size(namespace, setKey), count);

        // test delete() half
        sets.delete(namespace, setKey, 0, count / 2);
        final Map<String, Long> over50 = sets.get(namespace, setKey, 0, Long.MAX_VALUE, 0, -1, true);
        assertTrue(over50.values().stream().allMatch(w -> w > count / 2), "removing lower half, weight shouldn't be under " + count / 2);

        // empty set and verify
        sets.delete(namespace, setKey, Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(sets.size(namespace, setKey), 0);

        // test delete() in batch
        final List<String> batchRandoms = addRandoms(namespace, sets, setKey, count);
        assertEquals(sets.size(namespace, setKey), count);
        sets.delete(namespace, setKey, batchRandoms);
        final boolean anyPresent = sets.entries(namespace, setKey, Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, true).stream().anyMatch(batchRandoms::contains);
        assertFalse(anyPresent);
    }

    @Test
    public void testSize() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();

        assertEquals(sets.size(namespace, setKey), 0);

        int total = 0;
        for (int i = 0; i < 5; i++) {
            final int count = ThreadLocalRandom.current().nextInt(0, 100);
            addRandoms(namespace, sets, setKey, count);
            assertEquals(sets.size(namespace, setKey), total + count, "set size should increase by count when count random entries added");
            total += count;
        }
        assertEquals(sets.size(namespace, setKey), total, "set size should reflect total added");
        assertNotEquals(sets.size(namespace, setKey), total + ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE - total), "set size shouldn't match random int over total");
    }

    @Test
    public void testFirst() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();

        assertNull(sets.first(namespace, setKey));

        final String lowest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, lowest, 0L);
        assertEquals(sets.first(namespace, setKey), lowest, "lowest should be first");

        final String highest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, highest, 100L);
        assertEquals(sets.first(namespace, setKey), lowest, "lowest should be first");
        assertNotEquals(sets.first(namespace, setKey), highest, "highest shouldn't be first");

        final String middle = UUID.randomUUID().toString();
        sets.add(namespace, setKey, middle, 50L);
        assertEquals(sets.first(namespace, setKey), lowest, "lowest should be first");
        assertNotEquals(sets.first(namespace, setKey), middle, "middle shouldn't be first");

        assertTrue(sets.delete(namespace, setKey, lowest), "removing lowest shouldn't fail");
        assertEquals(sets.first(namespace, setKey), middle, "removed lowest so middle should be first");

        final String newLowest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, newLowest, 25L);
        assertEquals(sets.first(namespace, setKey), newLowest, "new lowest added, should be first");
        assertNotEquals(sets.first(namespace, setKey), middle, "middle is no longer first");
    }

    @Test
    public void testLast() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();

        assertNull(sets.last(namespace, setKey));

        final String lowest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, lowest, 0L);
        assertEquals(sets.last(namespace, setKey), lowest, "lowest should be last");

        final String highest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, highest, 100L);
        assertEquals(sets.last(namespace, setKey), highest, "highest should be last");
        assertNotEquals(sets.last(namespace, setKey), lowest, "lowest shouldn't be last");

        final String middle = UUID.randomUUID().toString();
        sets.add(namespace, setKey, middle, 50L);
        assertEquals(sets.last(namespace, setKey), highest, "highest should be last");
        assertNotEquals(sets.last(namespace, setKey), middle, "middle shouldn't be last");

        assertTrue(sets.delete(namespace, setKey, highest), "removing highest shouldn't fail");
        assertEquals(sets.last(namespace, setKey), middle, "removed lowest so middle should be last");
        assertNotEquals(sets.last(namespace, setKey), lowest, "lowest shouldn't be last");

        final String newHighest = UUID.randomUUID().toString();
        sets.add(namespace, setKey, newHighest, 75L);
        assertEquals(sets.last(namespace, setKey), newHighest, "new highest added, should be last");
        assertNotEquals(sets.last(namespace, setKey), middle, "middle is no longer last");
    }

    @Test
    public void testWeight() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();

        final List<String> keys = addRandoms(namespace, sets, setKey, getCount(100, 150));
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(sets.weight(namespace, setKey, keys.get(i)), Long.valueOf((long) i), "weight should return the correct weight in order");
        }

        // sets.weight must return null if entry does not exist
        assertNull(sets.weight(namespace, setKey, UUID.randomUUID().toString()));
    }

    @Test
    public void testInc() throws IOException {
        final Sets sets = getSets();

        final String setKey = UUID.randomUUID().toString();

        final String newEntry = UUID.randomUUID().toString();
        final long randomWeight = ThreadLocalRandom.current().nextLong();
        final long newEntryWeight = sets.inc(namespace, setKey, newEntry, randomWeight);
        assertEquals(newEntryWeight, randomWeight);

        final String entryToInc = UUID.randomUUID().toString();
        final String controlEntry = UUID.randomUUID().toString();

        sets.add(namespace, setKey, entryToInc, 0L);
        sets.add(namespace, setKey, controlEntry, 0L);

        long totalWeight = 0;
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10, 100); i++) {
            final long inc = ThreadLocalRandom.current().nextLong(0, 1000);
            final long returnedWeight;
            if (ThreadLocalRandom.current().nextBoolean()) {
                returnedWeight = sets.inc(namespace, setKey, entryToInc, inc);
                totalWeight += inc;
            } else {
                returnedWeight = sets.inc(namespace, setKey, entryToInc, inc * -1);
                totalWeight -= inc;
            }
            assertEquals(returnedWeight, totalWeight);
        }

        final Long incrementedWeight = sets.weight(namespace, setKey, entryToInc);
        assertNotNull(incrementedWeight, "entry should be in the set");
        assertEquals(incrementedWeight.longValue(), totalWeight, "entry weight should reflect increments");

        final Long unchangedWeight = sets.weight(namespace, setKey, controlEntry);
        assertNotNull(unchangedWeight, "control entry should be in the set");
        assertNotEquals(unchangedWeight, incrementedWeight, "incrementing shouldn't change control");
        assertEquals(unchangedWeight.longValue(), 0L, "control weight should still be 0");
    }

    private int getCount(final int min, final int max) {
        return (int) Math.floor(ThreadLocalRandom.current().nextInt(min, max) * getAddMagnitude());
    }

    private int getExactCount(final int min, final int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    private List<String> getRandoms(final int count) {
        final List<String> randoms = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String key = UUID.randomUUID().toString();
            randoms.add(key);
        }

        return randoms;
    }

    private List<String> addRandoms(final String namespace, final Sets sets, final String setKey, final int count) throws IOException {
        final List<String> randoms = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String key = UUID.randomUUID().toString();
            randoms.add(key);
            sets.add(namespace, setKey, key, i);
        }

        return randoms;
    }

    private <T> T getLast(Collection<T> col) {
        final Iterator<T> iterator = col.iterator();
        T last = null;
        while (iterator.hasNext()) {
            last = iterator.next();
        }

        return last;
    }

    private double getAddMagnitude() {
        return 1.0D;
    }

    private Sets getSets() throws IOException {
        return getCantor().sets();
    }
}

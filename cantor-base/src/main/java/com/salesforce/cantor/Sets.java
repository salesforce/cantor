/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Sets expose functionalities to work with sorted sets of strings. A set contains an ordered list of unique entries
 * and their associated weight in that particular set. Implementations of this interface allow users to add/remove
 * entries to/from sets, access and modify entries and their weights.
 */
public interface Sets extends Namespaceable {

    /**
     * Add an entry with a weight to the set in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the set to add the entry to
     * @param entry the entry
     * @param weight weight of the entry in the set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void add(String namespace, String set, String entry, long weight) throws IOException;

    /**
     * Add an entry with weight of 0 to the set in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the set to add the entry to
     * @param entry the entry
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default void add(String namespace, String set, String entry) throws IOException {
        add(namespace, set, entry, 0L);
    }

    /**
     * Add batch of entries/weights to the set in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the set to add the entry to
     * @param entries map of entries to weights
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void add(String namespace, String set, Map<String, Long> entries) throws IOException;

    /**
     * Return entries stored in the set with start and count and weight more than min, less than max.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param min the minimum weight
     * @param max the maximum weight
     * @param start start offset
     * @param count maximum number of entries to return
     * @param ascending ordered ascending or descending by weight
     * @return ordered list of entries in the set matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<String> entries(String namespace, String set, long min, long max, int start, int count, boolean ascending)
            throws IOException;

    /**
     * Return object entries and weights stored in the set with start and count and weight more than min, less than max
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param start start offset
     * @param count maximum number of entries to return
     * @param ascending ordered ascending or descending by weight
     * @return ordered list of entries in the set matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Collection<String> entries(String namespace, String set, int start, int count, boolean ascending)
            throws IOException {
        return entries(namespace, set, Long.MIN_VALUE, Long.MAX_VALUE, start, count, ascending);
    }

    /**
     * Return object entries and weights stored in the set with start and count and weight more than min, less than max
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param start start offset
     * @param count maximum number of entries to return
     * @return ordered list of entries in the set matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Collection<String> entries(String namespace, String set, int start, int count) throws IOException {
        return entries(namespace, set, start, count, true);
    }

    /**
     * Return object entries and weights stored in the set with start and count and weight more than min, less than max
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return ordered list of entries in the set matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Collection<String> entries(String namespace, String set) throws IOException {
        return entries(namespace, set, 0, -1);
    }

    /**
     * Return entries and weights stored in the set with start and count and weight more than min, less than max.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param min the minimum weight
     * @param max the maximum weight
     * @param start start offset
     * @param count maximum number of entries to return; -1 for infinite
     * @param ascending ordered ascending or descending by weight
     * @return map of entries to weights, matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Map<String, Long> get(String namespace, String set, long min, long max, int start, int count, boolean ascending)
            throws IOException;

    /**
     * Return all entries stored in the set, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return map of all entries to weights in the given set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> get(String namespace, String set)
            throws IOException {
        return get(namespace, set, 0, -1);
    }

    /**
     * Return entries stored in the set with start and count, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param start start offset
     * @param count maximum number of entries to return
     * @return paginated map of entries to weights
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> get(String namespace, String set, int start, int count)
            throws IOException {
        return get(namespace, set, Long.MIN_VALUE, Long.MAX_VALUE, start, count, true);
    }

    /**
     * Deletes all entries in the set with weights between the given min and max, in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param min the minimum weight
     * @param max the maximum weight
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void delete(String namespace, String set, long min, long max) throws IOException;

    /**
     * Deletes all entries in the set in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default void delete(String namespace, String set) throws IOException {
        delete(namespace, set, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Deletes an entry from the set in the given namespace.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return true if entry is found and deleted, false otherwise
     * @throws IOException exception thrown from the underlying storage implementation
     */
    boolean delete(String namespace, String set, String entry) throws IOException;

    /**
     * Delete batch of objects from set in the given namespace; no-op if an entry is not found.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void delete(String namespace, String set, Collection<String> entries) throws IOException;

    /**
     * Return union of entries and weights stored in any of the given sets,
     * with start and count and weight more than min, less than max.
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do union over
     * @param min the minimum weight
     * @param max the maximum weight
     * @param start start offset
     * @param count maximum number of entries to return; -1 for infinite
     * @param ascending ordered ascending or descending by weight
     * @return map of entries to weights, matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Map<String, Long> union(String namespace, Collection<String> sets, long min, long max, int start, int count, boolean ascending)
            throws IOException;

    /**
     * Return union of all entries stored in the given sets, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do union over
     * @return map of all entries to weights in the given set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> union(String namespace, Collection<String> sets)
            throws IOException {
        return union(namespace, sets, 0, -1);
    }

    /**
     * Return entries stored in the set with start and count, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do union over
     * @param start start offset
     * @param count maximum number of entries to return
     * @return paginated map of entries to weights
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> union(String namespace, Collection<String> sets, int start, int count)
            throws IOException {
        return union(namespace, sets, Long.MIN_VALUE, Long.MAX_VALUE, start, count, true);
    }

    /**
     * Return intersection of entries and weights stored in all of the given sets,
     * with start and count and weight more than min, less than max.
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do intersection over
     * @param min the minimum weight
     * @param max the maximum weight
     * @param start start offset
     * @param count maximum number of entries to return; -1 for infinite
     * @param ascending ordered ascending or descending by weight
     * @return map of entries to weights, matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Map<String, Long> intersect(String namespace, Collection<String> sets, long min, long max, int start, int count, boolean ascending)
            throws IOException;

    /**
     * Return intersect of all entries stored in the given sets, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do intersect over
     * @return map of all entries to weights in the given set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> intersect(String namespace, Collection<String> sets)
            throws IOException {
        return intersect(namespace, sets, 0, -1);
    }

    /**
     * Return entries stored in the set with start and count, ordered by weight ascending
     *
     * @param namespace the namespace
     * @param sets name of the sorted sets to do intersect over
     * @param start start offset
     * @param count maximum number of entries to return
     * @return paginated map of entries to weights
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> intersect(String namespace, Collection<String> sets, int start, int count)
            throws IOException {
        return intersect(namespace, sets, Long.MIN_VALUE, Long.MAX_VALUE, start, count, true);
    }

    /**
     * Return and atomically remove entries and weights stored in the set
     * with start and count and weight more than min, less than max.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param min the minimum weight
     * @param max the maximum weight
     * @param start start offset
     * @param count maximum number of entries to return; -1 for infinite
     * @param ascending ordered ascending or descending by weight
     * @return map of entries to weights, matching the given criteria
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Map<String, Long> pop(String namespace, String set, long min, long max, int start, int count, boolean ascending)
            throws IOException;

    /**
     * Return and atomically remove all entries in the set
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return map of all entries to weights in the given set
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> pop(String namespace, String set)
            throws IOException {
        return pop(namespace, set, 0, -1);
    }

    /**
     * Return and atomically remove all entries in the set with start and count
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param start start offset
     * @param count maximum number of entries to return
     * @return paginated map of entries to weights
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, Long> pop(String namespace, String set, int start, int count)
            throws IOException {
        return pop(namespace, set, Long.MIN_VALUE, Long.MAX_VALUE, start, count, true);
    }

    /**
     * Returns list of all sets in the given namespace.
     *
     * @param namespace the namespace
     * @return list of all sets in the namespace
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<String> sets(String namespace) throws IOException;

    /**
     * Get the number of entries in the set.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return the number of entries in the set in the namespace
     * @throws IOException exception thrown from the underlying storage implementation
     */
    int size(String namespace, String set) throws IOException;

    /**
     * Get the first entry in the set ordered ascending by weight.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return the first entry in the set in the namespace if found; null otherwise
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default String first(final String namespace, final String set) throws IOException {
        final Collection<String> results = entries(namespace, set, 0, 1, true);
        if (results == null || results.isEmpty()) {
            return null;
        }
        return results.iterator().next();
    }

    /**
     * Get the last entry in the set ordered ascending by weight.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return the last entry in the set in the namespace if found; null otherwise
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default String last(final String namespace, final String set) throws IOException {
        final Collection<String> results = entries(namespace, set, 0, 1, false);
        if (results == null || results.isEmpty()) {
            return null;
        }
        return results.iterator().next();
    }

    /**
     * Returns an entry's weight in the set.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @return the weight associated to the set in the entry in the namespace
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Long weight(String namespace, String set, String entry) throws IOException;

    /**
     * Atomic operation to increment an entry's weight in the set by the given count
     * and then return the final value.
     *
     * @param namespace the namespace
     * @param set name of the sorted set
     * @param count count to increment the weight by; use negative values to decrement
     * @throws IOException exception thrown from the underlying storage implementation
     * @return final weight of the entry after incrementing
     */
    long inc(String namespace, String set, String entry, long count) throws IOException;
}


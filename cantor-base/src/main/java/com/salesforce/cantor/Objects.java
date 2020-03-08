/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Objects expose functionalities to work with key/value pairs.
 */
public interface Objects {

    /**
     * Get list of all namespaces.
     *
     * @return collection of namespace identifiers
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<String> namespaces() throws IOException;

    /**
     * Create a new namespace.
     *
     * @param namespace the namespace identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void create(String namespace) throws IOException;

    /**
     * Drop a namespace.
     *
     * @param namespace the namespace identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void drop(String namespace) throws IOException;

    /**
     * Stores bytes for the given key.
     *
     * @param namespace the namespace identifier
     * @param key the key
     * @param bytes the value in bytes
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void store(String namespace, String key, byte[] bytes) throws IOException;

    /**
     * Stores batch of key/value pairs.
     *
     * @param namespace the namespace identifier
     * @param batch batch of key/value pairs
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default void store(String namespace, Map<String, byte[]> batch) throws IOException {
        if (namespace == null || namespace.length() == 0) {
            throw new IllegalArgumentException("null/empty namespace");
        }
        if (batch == null) {
            throw new IllegalArgumentException("null batch");
        }
        for (final Map.Entry<String, byte[]> object : batch.entrySet()) {
            store(namespace, object.getKey(), object.getValue());
        }
    }

    /**
     * Returns bytes associated to the given key.
     *
     * @param namespace the namespace identifier
     * @param key the key
     * @return bytes associated to the key in the given namespace; null if not found
     * @throws IOException exception thrown from the underlying storage implementation
     */
    byte[] get(String namespace, String key) throws IOException;

    /**
     * Returns batch of key/values for the list of entries.
     *
     * @param namespace the namespace identifier
     * @param keys batch of keys
     * @return map of key/value pairs for the given list of keys in the namespace; value is set to null if not found
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default Map<String, byte[]> get(String namespace, Collection<String> keys) throws IOException {
        if (namespace == null || namespace.length() == 0) {
            throw new IllegalArgumentException("null/empty namespace");
        }
        if (keys == null) {
            throw new IllegalArgumentException("null keys");
        }
        final Map<String, byte[]> results = new HashMap<>();
        for (final String key : keys) {
            results.put(key, get(namespace, key));
        }
        return results;
    }

    /**
     * Delete the object; return true if object was found and removed successfully, false otherwise.
     *
     * @param namespace the namespace identifier
     * @param key the key
     * @return true if key is found and deleted, false otherwise
     * @throws IOException exception thrown from the underlying storage implementation
     */
    boolean delete(String namespace, String key) throws IOException;

    /**
     * Delete batch of objects.
     *
     * @param namespace the namespace identifier
     * @param keys batch of keys
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default void delete(String namespace, Collection<String> keys) throws IOException {
        if (namespace == null || namespace.length() == 0) {
            throw new IllegalArgumentException("null/empty namespace");
        }
        if (keys == null) {
            throw new IllegalArgumentException("null keys");
        }
        for (final String key : keys) {
            delete(namespace, key);
        }
    }

    /**
     * Returns paginated list of entries; the returned list is not ordered.
     *
     * @param namespace the namespace identifier
     * @param start start offset
     * @param count maximum number of entries to return; -1 for infinite
     * @return paginated list of keys in the namespace
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<String> keys(String namespace, int start, int count) throws IOException;

    /**
     * Returns number of key/value pairs in the given namespace.
     *
     * @param namespace the namespace identifier
     * @return number of objects in the namespace
     * @throws IOException exception thrown from the underlying storage implementation
     */
    int size(String namespace) throws IOException;

}


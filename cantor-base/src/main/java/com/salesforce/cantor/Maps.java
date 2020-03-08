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
 * Maps expose functionalities to work with map of strings to string.
 */
public interface Maps {

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
     * Store a map in the given namespace.
     *
     * @param namespace the namespace identifier
     * @param map the map to store
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void store(String namespace, Map<String, String> map) throws IOException;

    /**
     * Get all maps matching the given query object.
     *
     * @param namespace the namespace identifier
     * @param query query object to match against
     * @return all maps matching the query object
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<Map<String, String>> get(String namespace, Map<String, String> query) throws IOException;

    /**
     * Delete and return the count of all maps matching the given query object.
     *
     * @param namespace the namespace identifier
     * @param query query object to match against
     * @return all maps matching the query object
     * @throws IOException exception thrown from the underlying storage implementation
     */
    int delete(String namespace, Map<String, String> query) throws IOException;
}


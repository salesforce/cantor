/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.functions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;

/**
 * Functions allow users to store/retrieve and execute a function in this sandbox.
 */
public interface Functions {

    /**
     * Create a new function namespace.
     *
     * @param namespace the namespace identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void create(String namespace) throws IOException;

    /**
     * Drop a function namespace.
     *
     * @param namespace the namespace identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void drop(String namespace) throws IOException;

    /**
     * Store a function with the given name and body as string.
     *
     * @param namespace the namespace identifier
     * @param function the function name identifier
     * @param body body of the function
     * @throws IOException exception thrown from the underlying storage implementation
     */
    default void store(final String namespace, final String function, final String body) throws IOException {
        checkString(body, "missing function body", namespace);
        store(namespace, function, body.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Store a function with the given name and body as byte array.
     *
     * @param namespace the namespace identifier
     * @param function the function name identifier
     * @param body body of the function
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void store(String namespace, String function, byte[] body) throws IOException;

    /**
     * Retrieve the function body from the given namespace.
     *
     * @param namespace the namespace identifier
     * @param function the function name identifier
     * @return byte array representation of the body of the function; null if not found
     * @throws IOException exception thrown from the underlying storage implementation
     */
    byte[] get(String namespace, String function) throws IOException;

    /**
     * Delete the function from the given namespace.
     *
     * @param namespace the namespace identifier
     * @param function the function name identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void delete(String namespace, String function) throws IOException;

    /**
     * Get the list of all functions in the given namespace.
     *
     * @param namespace the namespace identifier
     * @throws IOException exception thrown from the underlying storage implementation
     */
    Collection<String> list(String namespace) throws IOException;

    /**
     * Execute the function, given the context and param arguments.
     *
     * @param namespace the namespace identifier
     * @param function the function name identifier
     * @param context the context variables to pass to function on execution
     * @param params the map of parameters to pass to function on execution
     * @throws IOException exception thrown from the underlying storage implementation
     */
    void run(String namespace, String function, Context context, Map<String, String> params) throws IOException;
}

/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor;

import java.io.IOException;
import java.util.Collection;

public interface Namespaceable {
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
}

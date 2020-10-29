/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.s3;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.io.InputStream;

/**
 * Extension of the {@link Objects} interface to implement storing/streaming objects that are too large
 * to store/get as whole {@link byte[]}.
 */
public interface StreamingObjects extends Objects {
    /**
     * Stores the given object by streaming the content to the underlying storage
     * @param namespace the namespace to store in
     * @param key the key for the object
     * @param stream the content of the object as an {@link InputStream}
     * @param length the full length of the content
     */
    void store(String namespace, String key, InputStream stream, long length) throws IOException;

    /**
     * Gets the object corresponding to the given namespace/key as an {@link InputStream}
     * @param namespace the namespace of the object
     * @param key the key of the object
     * @return an {@link InputStream} for the stored object/null if it doesn't exist
     */
    InputStream stream(String namespace, String key) throws IOException;
}

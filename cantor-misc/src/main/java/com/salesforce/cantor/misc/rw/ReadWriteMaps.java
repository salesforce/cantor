/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.MapsPreconditions.*;

public class ReadWriteMaps implements Maps {
    private final Maps writable;
    private final Maps readable;

    public ReadWriteMaps(final Maps writable, final Maps readable) {
        checkArgument(writable != null, "null writable");
        checkArgument(readable != null, "null readable");
        this.writable = writable;
        this.readable = readable;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return this.readable.namespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        this.writable.create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        this.writable.drop(namespace);
    }

    @Override
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        this.writable.store(namespace, map);
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return this.readable.get(namespace, query);
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkDelete(namespace, query);
        return this.writable.delete(namespace, query);
    }
}

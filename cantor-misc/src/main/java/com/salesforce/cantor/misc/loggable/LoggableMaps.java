/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import com.salesforce.cantor.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.MapsPreconditions.*;

public class LoggableMaps extends AbstractBaseLoggableCantor implements Maps {
    private final Maps delegate;

    public LoggableMaps(final Maps delegate) {
        checkArgument(delegate != null, "null delegate");
        this.delegate = delegate;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return logCall(this.delegate::namespaces, "namespaces", null);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        logCall(() -> { this.delegate.create(namespace); return null; },
                "create", namespace
        );
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        logCall(() -> { this.delegate.drop(namespace); return null; },
                "drop", namespace
        );
    }

    @Override
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        logCall(() -> { this.delegate.store(namespace, map); return null; },
                "store", namespace
        );
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return logCall(() -> this.delegate.get(namespace, query),
                "get", namespace
        );
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkDelete(namespace, query);
        return logCall(() -> this.delegate.delete(namespace, query),
                "delete", namespace
        );
    }
}


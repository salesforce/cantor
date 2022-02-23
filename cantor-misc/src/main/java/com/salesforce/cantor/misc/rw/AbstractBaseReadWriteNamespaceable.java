/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.Collection;

import static com.salesforce.cantor.common.CommonPreconditions.*;

abstract class AbstractBaseReadWriteNamespaceable<T extends Namespaceable> implements Namespaceable {
    private final T writable;
    private final T readable;

    AbstractBaseReadWriteNamespaceable(final T writable, final T readable) {
        checkArgument(writable != null, "null writable");
        checkArgument(readable != null, "null readable");
        this.writable = writable;
        this.readable = readable;
    }

    @Override
    public final void create(final String namespace) throws IOException {
        checkCreate(namespace);
        getWritable().create(namespace);
    }

    @Override
    public final void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        getWritable().drop(namespace);
    }

    protected T getWritable() {
        return this.writable;
    }

    protected T getReadable() {
        return this.readable;
    }
}

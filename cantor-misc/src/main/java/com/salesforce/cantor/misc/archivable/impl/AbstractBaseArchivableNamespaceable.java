/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable.impl;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.Collection;

abstract class AbstractBaseArchivableNamespaceable<T extends Namespaceable, A extends Namespaceable>
                implements Namespaceable {
    private final T delegate;
    private final A archiveDelegate;

    public AbstractBaseArchivableNamespaceable(final T delegate, final A archiveDelegate) {
        this.delegate = delegate;
        this.archiveDelegate = archiveDelegate;
    }

    @Override
    public void create(final String namespace) throws IOException {
        getArchiver().create(namespace);
        getDelegate().create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        getArchiver().drop(namespace);
        getDelegate().drop(namespace);
    }

    protected T getDelegate() {
        return this.delegate;
    }

    protected A getArchiver() {
        return this.archiveDelegate;
    }
}

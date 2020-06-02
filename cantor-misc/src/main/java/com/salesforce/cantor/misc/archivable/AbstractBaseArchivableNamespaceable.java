/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.Collection;

abstract class AbstractBaseArchivableNamespaceable<D extends Namespaceable, T extends EventsArchiver>
                implements Namespaceable {
    private final D delegate;
    private final T archiveDelegate;

    public AbstractBaseArchivableNamespaceable(final D delegate, final T archiveDelegate) {
        this.delegate = delegate;
        this.archiveDelegate = archiveDelegate;
    }

    @Override
    public final Collection<String> namespaces() throws IOException {
        return getDelegate().namespaces();
    }

    @Override
    public final void create(final String namespace) throws IOException {
        getDelegate().create(namespace);
    }

    @Override
    public final void drop(final String namespace) throws IOException {
        getDelegate().drop(namespace);
    }

    protected D getDelegate() {
        return this.delegate;
    }

    protected T getArchiveDelegate() {
        return this.archiveDelegate;
    }
}

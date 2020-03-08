/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.concurrent.*;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

abstract class AbstractBaseAsyncNamespaceable<T extends Namespaceable> implements Namespaceable {
    private final T delegate;
    private final ExecutorService executorService;

    AbstractBaseAsyncNamespaceable(final T delegate, final ExecutorService executorService) {
        checkArgument(delegate != null, "null delegate");
        checkArgument(executorService != null, "null executor service");
        this.executorService = executorService;
        this.delegate = delegate;
    }

    <R> R submitCall(final Callable<R> callable) throws IOException {
        // construct the name for this thread
        // the thread name reflects the method, namespace, and parameters
        final Future<R> future = this.executorService.submit(callable);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    protected T getDelegate() {
        return this.delegate;
    }
}


/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Namespaceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

abstract class AbstractBaseShardedNamespaceable<T extends Namespaceable> implements Namespaceable {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final T[] delegates;

    public AbstractBaseShardedNamespaceable(final T[] delegates) {
        checkArgument(delegates != null && delegates.length > 0, "null/empty delegates");
        this.delegates = delegates;
    }

    protected T getShard(final String namespace) throws IOException {
        return this.delegates[Math.abs(namespace.hashCode()) % this.delegates.length];
    }

}

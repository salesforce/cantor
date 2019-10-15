/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

class Shardeds {
    static <C> C getShard(final C[] delegates, final String namespace) {
        return delegates[Math.abs(namespace.hashCode()) % delegates.length];
    }
}

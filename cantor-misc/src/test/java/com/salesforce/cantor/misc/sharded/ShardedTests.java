/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;

import java.io.IOException;
import java.util.UUID;

class ShardedTests {
    private static final String path = "/tmp/cantor-sharded-test/" + UUID.randomUUID().toString();
    private static final Cantor cantor;
    private static final int shardsCount = 3;

    static {
        final Cantor[] shards = new Cantor[shardsCount];
        for (int i = 0; i < shardsCount; ++i) {
            try {
                shards[i] = new CantorOnH2(String.format("%s/shard-%d", path, i));
            } catch (IOException e) {}
        }
        cantor = new ShardedCantor(shards);
    }

    public static Cantor getCantor() {
        return cantor;
    }
}

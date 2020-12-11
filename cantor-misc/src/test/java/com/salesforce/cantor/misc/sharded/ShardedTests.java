/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.common.providers.Factory;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.h2.EventsOnH2;
import com.salesforce.cantor.mysql.CantorOnMysql;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.UUID;

class ShardedTests {
    private static final String path = "/tmp/cantor-sharded-test/" + UUID.randomUUID().toString();
    private static final Cantor cantor;
    private static final int shardsCount = 5;

    static {
        final Cantor[] shards = new Cantor[shardsCount];
        for (int i = 0; i < shardsCount; ++i) {
            try {
                shards[i] = new CantorOnH2(String.format("%s/shard-%d", path, i));
//                shards[i] = new CantorOnMysql("localhost", 3306 + i, null, null);
            } catch (IOException e) {}
        }
        cantor = new ShardedCantor(shards);
    }

    public static Cantor getCantor() {
        return cantor;
    }


    public static void main(String[] args) throws IOException {
        final Events proxyInstance = (Events) Proxy.newProxyInstance(
                Factory.CantorInvocationHandler.class.getClassLoader(),
                new Class[] { Events.class },
                new Factory.CantorInvocationHandler(new EventsOnH2("/tmp/foo"))
        );
        proxyInstance.create("s3.foo");
        System.out.println("proxy call: " + proxyInstance.namespaces());
    }
}

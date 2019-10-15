/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.server.CantorEnvironment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.util.concurrent.ThreadLocalRandom;

import static com.salesforce.cantor.server.Constants.PATH_CONFIG_FILE;

class GrpcTests {
    private static final int randomPort = ThreadLocalRandom.current().nextInt(10000, 30000);
    private static final Cantor cantor = new CantorOnGrpc("localhost:" + randomPort);

    static Cantor getCantor() {
        return cantor;
    }

    static CantorEnvironment getTestCantorProperties() {
        final Config cantorProperties = ConfigFactory
                .load(PATH_CONFIG_FILE)
                .withValue("cantor.grpc.port", ConfigValueFactory.fromAnyRef(randomPort));
        return new CantorEnvironment(cantorProperties);
    }
}

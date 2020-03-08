/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class MetricCollectingTests {
    private static final String path = "/tmp/cantor-metric-collecting-test/" + UUID.randomUUID().toString();

    public static Cantor getCantor() throws IOException {
        final MetricRegistry registry = new MetricRegistry();
        Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger(MetricCollectingTests.class))
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
                .build()
                .start(1L, TimeUnit.SECONDS);
        return new MetricCollectingCantor(registry, new CantorOnH2(Paths.get(path, "cantor").toString()));
    }
}

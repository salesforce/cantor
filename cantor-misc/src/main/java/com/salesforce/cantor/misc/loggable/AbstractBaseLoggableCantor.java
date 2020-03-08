/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

class AbstractBaseLoggableCantor {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    <R> R logCall(final Callable<R> callable,
                  final String methodName,
                  final String namespace,
                  final Object... parameters) throws IOException {
        final long startNanos = System.nanoTime();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            logger.info("'{}.{}('{}', {}); time spent: {}ms",
                    getClass().getSimpleName(),
                    methodName,
                    namespace,
                    parameters,
                    ((System.nanoTime() - startNanos) / 1_000_000)  // nanos to millis
            );
        }
    }
}

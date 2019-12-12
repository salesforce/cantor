/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class BaseMetricCollectingCantor {
    private static final Logger logger = LoggerFactory.getLogger(BaseMetricCollectingCantor.class);
    private final MetricRegistry metrics;
    private final Object delegate;

    BaseMetricCollectingCantor(final MetricRegistry metrics, final Object delegate) {
        this.metrics = metrics;
        this.delegate = delegate;
    }

    // collects timer metrics and uses the given function to transform the cantor result into an integer for a histogram
    <R> R metrics(final Callable<R> callable, final String methodName, final String namespace, final Function<R, Integer> resultToHistogramValue) throws IOException {
        final R result = metrics(callable, methodName, namespace);
        try {
            final Integer value = resultToHistogramValue.apply(result);
            if (value != null) {
                this.metrics.histogram(getHistogramName(namespace, methodName)).update(value);
            }
        } catch (final Exception e) {
           logger.warn("exception transforming result to histogram value: ", e);
        }
        return result;
    }

    <R> R metrics(final Callable<R> callable, final String methodName, final String namespace) throws IOException {
        try {
            return metrics.timer(getTimerName(namespace, methodName)).time(callable);
        } catch (final IllegalArgumentException e) {
            // rethrow illegal arguments, so we don't have to do checks ourselves...
            throw e;
        } catch (final Exception e) {
            // todo: this swallows any metric from the timer as IOException, should we replace?
            throw new IOException(e);
        }
    }

    void metrics(final IORunnable runnable, final String methodName, final String namespace) throws IOException {
        try (final Timer.Context ignored = metrics.timer(getTimerName(namespace, methodName)).time()) {
            runnable.run();
        } catch (final IllegalArgumentException e) {
            // rethrow illegal arguments, so we don't have to do checks ourselves...
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    Integer size(final Collection<?> collection) {
        return collection != null ? collection.size() : 0;
    }

    private String getTimerName(final String namespace, final String method) {
        return MetricRegistry.name(this.delegate.getClass(), namespace, method, "calls");
    }

    private String getHistogramName(final String namespace, final String method) {
        return MetricRegistry.name(this.delegate.getClass(), namespace, method, "response-size");
    }

    interface IORunnable {
        void run() throws IOException;
    }
}

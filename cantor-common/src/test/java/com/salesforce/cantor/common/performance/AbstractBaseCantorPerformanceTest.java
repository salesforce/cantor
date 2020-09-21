/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common.performance;

import com.salesforce.cantor.Cantor;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.DoubleStream;

public abstract class AbstractBaseCantorPerformanceTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract Cantor getCantor() throws IOException;

    protected void printStatsTable(final String title, final Map<String, Percentile> percentiles) {
        System.out.println("-----------------------------------------------------------------------------------------------");
        System.out.printf("%20s|%7s %7s %7s %7s %7s %7s %7s %7s %7s%n", title, "Count", "Sum", "Avg", "Min", "Max", "P50", "P90", "P95", "P99");
        System.out.println("-----------------------------------------------------------------------------------------------");

        final File file = new File(this.getClass().getSimpleName() + "_" + title);
        try (final OutputStream csvFile = new FileOutputStream(file)) {
            csvFile.write(String.format("%s,Count,Sum,Avg,Min,Max,P50,P90,P95,P99%n", title).getBytes());
            csvFile.flush();
            for (final String type : percentiles.keySet()) {
                final Percentile percentile = percentiles.get(type);
                final double[] data = percentile.getData();
                final DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
                for (final double datum : data) {
                    summaryStatistics.accept(datum);
                }

                System.out.printf("%20s|%7d %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f%n",
                    type,
                    summaryStatistics.getCount(),
                    summaryStatistics.getSum(),
                    summaryStatistics.getAverage(),
                    summaryStatistics.getMin(),
                    summaryStatistics.getMax(),
                    percentile.evaluate(50),
                    percentile.evaluate(90),
                    percentile.evaluate(95),
                    percentile.evaluate(99)
                );

                csvFile.write(String.format("%s,%d,%f,%f,%f,%f,%f,%f,%f,%f%n",
                    type,
                    summaryStatistics.getCount(),
                    summaryStatistics.getSum(),
                    summaryStatistics.getAverage(),
                    summaryStatistics.getMin(),
                    summaryStatistics.getMax(),
                    percentile.evaluate(50),
                    percentile.evaluate(90),
                    percentile.evaluate(95),
                    percentile.evaluate(99)
                ).getBytes());
            }
            csvFile.flush();

            final Map<String, double[]> crossIterationPercentile = new TreeMap<>();
            for (final String type : percentiles.keySet()) {
                final Percentile percentile = percentiles.get(type);
                final String key = type.substring(0, type.indexOf("-"));
                crossIterationPercentile.compute(key, (k, value) -> (value == null)
                        ? percentile.getData()
                        : DoubleStream.concat(DoubleStream.of(value), DoubleStream.of(percentile.getData())).toArray());
            }

            for (final String type : crossIterationPercentile.keySet()) {
                final Percentile percentile = new Percentile();
                percentile.setData(crossIterationPercentile.get(type));
                final DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
                for (final double datum : percentile.getData()) {
                    summaryStatistics.accept(datum);
                }

                System.out.printf("%20s|%7d %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f%n",
                        type,
                        summaryStatistics.getCount(),
                        summaryStatistics.getSum(),
                        summaryStatistics.getAverage(),
                        summaryStatistics.getMin(),
                        summaryStatistics.getMax(),
                        percentile.evaluate(50),
                        percentile.evaluate(90),
                        percentile.evaluate(95),
                        percentile.evaluate(99)
                );

                csvFile.write(String.format("%s,%d,%f,%f,%f,%f,%f,%f,%f,%f%n",
                        type,
                        summaryStatistics.getCount(),
                        summaryStatistics.getSum(),
                        summaryStatistics.getAverage(),
                        summaryStatistics.getMin(),
                        summaryStatistics.getMax(),
                        percentile.evaluate(50),
                        percentile.evaluate(90),
                        percentile.evaluate(95),
                        percentile.evaluate(99)
                ).getBytes());
            }
            csvFile.flush();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }
}

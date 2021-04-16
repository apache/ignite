/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jol;

import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.openjdk.jol.info.GraphLayout;

/**
 * Benchmark to measure heap space for metrics.
 */
public class GridMetricsJolBenchmark {
    /** */
    private static final int BOOLEAN_CNT = 100;

    /** */
    private static final int DOUBLE_CNT = 100;

    /** */
    private static final int INT_CNT = 100;

    /** */
    private static final int LONG_CNT = 100;

    /** */
    private static final int LONG_ADDER_CNT = 100;

    /** */
    private static final int TOTAL = BOOLEAN_CNT + DOUBLE_CNT + INT_CNT + LONG_CNT + LONG_ADDER_CNT;

    /** */
    public static final String BOOLEAN_METRIC = "boolean.metric.";

    /** */
    public static final String DOUBLE_METRIC = "double.metric.";

    /** */
    public static final String INT_METRIC = "int.metric.";

    /** */
    public static final String LONG_METRIC = "long.metric.";

    /** */
    public static final String LONG_ADDER_METRIC = "long.adder.metric.";

    /** */
    public static void main(String[] args) {
        measureMetricRegistry();

        measureArray();
    }

    /**
     * Calculates and prints the size of metrics array of {@code TOTAL} size;
     */
    private static void measureArray() {
        Object[] metrics = new Object[TOTAL];

        int start = 0;

        for (int i = 0; i < BOOLEAN_CNT; i++)
            metrics[start + i] = new BooleanMetricImpl(BOOLEAN_METRIC + i, null);

        start += BOOLEAN_CNT;

        for (int i = 0; i < DOUBLE_CNT; i++)
            metrics[start + i] = new DoubleMetricImpl(DOUBLE_METRIC + i, null);

        start += DOUBLE_CNT;

        for (int i = 0; i < INT_CNT; i++)
            metrics[start + i] = new IntMetricImpl(INT_METRIC + i, null);

        start += INT_CNT;

        for (int i = 0; i < LONG_CNT; i++)
            metrics[start + i] = new AtomicLongMetric(LONG_METRIC + i, null);

        start += LONG_CNT;

        for (int i = 0; i < LONG_ADDER_CNT; i++)
            metrics[start + i] = new LongAdderMetric(LONG_ADDER_METRIC + i, null);

        start += LONG_ADDER_CNT;

        long sz = GraphLayout.parseInstance(metrics).totalSize();

        System.out.println("Total size of " + TOTAL + " metric array is " + (sz / 1024) + "KiB, " + sz + " bytes.");
    }

    /**
     * Calculates and prints the size of metric registry of {@code TOTAL} size;
     */
    private static void measureMetricRegistry() {
        MetricRegistry mreg = new MetricRegistry("test", name -> null, name -> null, null);

        for (int i = 0; i < BOOLEAN_CNT; i++)
            mreg.booleanMetric(BOOLEAN_METRIC + i, null);

        for (int i = 0; i < DOUBLE_CNT; i++)
            mreg.doubleMetric(DOUBLE_METRIC + i, null);

        for (int i = 0; i < INT_CNT; i++)
            mreg.doubleMetric(INT_METRIC + i, null);

        for (int i = 0; i < LONG_CNT; i++)
            mreg.longMetric(LONG_METRIC + i, null);

        for (int i = 0; i < LONG_ADDER_CNT; i++)
            mreg.longMetric(LONG_ADDER_METRIC + i, null);

        long sz = GraphLayout.parseInstance(mreg).totalSize();

        System.out.println("Total size of " + TOTAL + " metric registry is " + (sz / 1024) + "KiB, " + sz + " bytes.");
    }
}

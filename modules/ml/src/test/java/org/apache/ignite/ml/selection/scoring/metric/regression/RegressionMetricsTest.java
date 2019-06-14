/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.selection.scoring.metric.regression;

import java.util.Arrays;
import org.apache.ignite.ml.selection.scoring.TestLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RegressionMetrics}.
 */
public class RegressionMetricsTest {
    /**
     *
     */
    @Test
    public void testDefaultBehaviour() {
        Metric scoreCalculator = new RegressionMetrics();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.5, score, 1e-12);
    }

    /**
     *
     */
    @Test
    public void testDefaultBehaviourForScoreAll() {
        RegressionMetrics scoreCalculator = new RegressionMetrics();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        RegressionMetricValues metricValues = scoreCalculator.scoreAll(cursor.iterator());

        assertEquals(1.0, metricValues.rss(), 1e-12);
    }

    /**
     *
     */
    @Test
    public void testCustomMetric() {
        RegressionMetrics scoreCalculator = (RegressionMetrics) new RegressionMetrics()
            .withMetric(RegressionMetricValues::mae);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.25, score, 1e-12);
    }

    /**
     *
     */
    @Test
    public void testNullCustomMetric() {
        RegressionMetrics scoreCalculator = (RegressionMetrics) new RegressionMetrics()
            .withMetric(null);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        // rmse as default metric
        assertEquals(0.5, score, 1e-12);
    }

    /**
     *
     */
    @Test
    public void testR2_1() {
        RegressionMetrics scoreCalculator = (RegressionMetrics) new RegressionMetrics()
            .withMetric(RegressionMetricValues::r2);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.0, score, 1e-12);
    }

    /**
     *
     */
    @Test
    public void testR2_2() {
        RegressionMetrics scoreCalculator = (RegressionMetrics) new RegressionMetrics()
            .withMetric(RegressionMetricValues::r2);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 2.0, 3.0, 4.0),
            Arrays.asList(2.0, 2.0, 5.0, 10.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(-7.19, score, 0.01);
    }
}

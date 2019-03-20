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

package org.apache.ignite.ml.selection.scoring.metric.classification;

import org.apache.ignite.ml.selection.scoring.TestLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.scoring.metric.exceptions.UnknownClassLabelException;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RegressionMetrics}.
 */
public class BinaryClassificationMetricsTest {
    /** */
    @Test
    public void testDefaultBehaviour() {
        Metric scoreCalculator = new BinaryClassificationMetrics();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.75, score, 1e-12);
    }

    /** */
    @Test
    public void testDefaultBehaviourForScoreAll() {
        BinaryClassificationMetrics scoreCalculator = new BinaryClassificationMetrics();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        BinaryClassificationMetricValues metricValues = scoreCalculator.scoreAll(cursor.iterator());

        assertEquals(0.75, metricValues.accuracy(), 1e-12);
    }

    /** */
    @Test
    public void testAccuracy() {
        Metric scoreCalculator = new BinaryClassificationMetrics()
            .withNegativeClsLb(1.0)
            .withPositiveClsLb(2.0);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.75, score, 1e-12);
    }

    /** */
    @Test
    public void testCustomMetric() {
        Metric scoreCalculator = new BinaryClassificationMetrics()
            .withNegativeClsLb(1.0)
            .withPositiveClsLb(2.0)
            .withMetric(BinaryClassificationMetricValues::tp);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(3, score, 1e-12);
    }

    /** */
    @Test
    public void testNullCustomMetric() {
        Metric scoreCalculator = new BinaryClassificationMetrics()
            .withNegativeClsLb(1.0)
            .withPositiveClsLb(2.0)
            .withMetric(null);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        // accuracy as default metric
        assertEquals(0.75, score, 1e-12);
    }

    /** */
    @Test
    public void testNaNinClassLabels() {
        Metric scoreCalculator = new BinaryClassificationMetrics()
            .withNegativeClsLb(Double.NaN)
            .withPositiveClsLb(Double.POSITIVE_INFINITY);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        // accuracy as default metric
        assertEquals(0.75, score, 1e-12);
    }

    /** */
    @Test(expected = UnknownClassLabelException.class)
    public void testFailWithIncorrectClassLabelsInData() {
        Metric scoreCalculator = new BinaryClassificationMetrics();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(2.0, 2.0, 2.0, 2.0),
            Arrays.asList(2.0, 2.0, 1.0, 2.0)
        );

        scoreCalculator.score(cursor.iterator());
    }

    /** */
    @Test(expected = UnknownClassLabelException.class)
    public void testFailWithIncorrectClassLabelsInMetrics() {
        Metric scoreCalculator = new BinaryClassificationMetrics()
            .withPositiveClsLb(42);

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        scoreCalculator.score(cursor.iterator());
    }
}

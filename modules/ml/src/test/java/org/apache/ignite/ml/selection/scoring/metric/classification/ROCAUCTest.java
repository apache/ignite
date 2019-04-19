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

package org.apache.ignite.ml.selection.scoring.metric.classification;

import java.util.Arrays;
import org.apache.ignite.ml.selection.scoring.TestLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ROCAUC}.
 */
public class ROCAUCTest {
    /** */
    @Test
    public void testTotalTruth() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0),
            Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(Double.NaN, score, 1e-12);
    }

    /** */
    @Test
    public void testTotalUntruth() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 1.0, 1.0),
            Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(Double.NaN, score, 1e-12);
    }

    /** */
    @Test
    public void testOneDifferent() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 0.0),
            Arrays.asList(1.0, 1.0, 1.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.5, score, 1e-12);
    }

    /** */
    @Test
    public void testOneDifferentButBalanced() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 0.0, 0.0),
            Arrays.asList(1.0, 1.0, 0.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.75, score, 1e-12);
    }

    /** */
    @Test
    public void testTwoDifferentAndBalanced() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 0.0, 0.0),
            Arrays.asList(1.0, 0.0, 0.0, 1.0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.5, score, 1e-12);
    }

    /** */
    @Test
    public void testNotOnlyBinaryValues() {
        Metric<Double> scoreCalculator = new ROCAUC();

        LabelPairCursor<Double> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0),
            Arrays.asList(0.40209054, 0.33697626, 0.5449324 , 0.13010869, 0.19019675, 0.39767829, 0.9686739 , 0.91783275, 0.7503783 , 0.5306605)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.625, score, 1e-12);
    }
}

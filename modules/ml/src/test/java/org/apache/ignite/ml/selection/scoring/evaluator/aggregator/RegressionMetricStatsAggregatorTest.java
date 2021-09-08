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

package org.apache.ignite.ml.selection.scoring.evaluator.aggregator;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RegressionMetricStatsAggregator} class.
 */
public class RegressionMetricStatsAggregatorTest {
    /**
     *
     */
    @Test
    public void itShouldBeCarefulWithNaNs() {
        RegressionMetricStatsAggregator agg = new RegressionMetricStatsAggregator();
        assertEquals(Double.NaN, agg.getMAE(), 0.);
        assertEquals(Double.NaN, agg.getMSE(), 0.);
        assertEquals(Double.NaN, agg.getRss(), 0.);
        assertEquals(Double.NaN, agg.ysVariance(), 0.);
        assertEquals(Double.NaN, agg.ysRss(), 0.);

        agg.aggregate(mdl, VectorUtils.of(1.).labeled(1.));
        assertEquals(0., agg.getMAE(), 0.);
        assertEquals(0., agg.getMSE(), 0.);
        assertEquals(0., agg.getRss(), 0.);
        assertEquals(0., agg.ysVariance(), 0.);
        assertEquals(0., agg.ysRss(), 0.);
    }

    /**
     *
     */
    @Test
    public void testAggregate() {
        RegressionMetricStatsAggregator agg = new RegressionMetricStatsAggregator();

        agg.aggregate(mdl, VectorUtils.of(1.).labeled(2.));
        agg.aggregate(mdl, VectorUtils.of(2.).labeled(4.));
        agg.aggregate(mdl, VectorUtils.of(3.).labeled(6.));
        assertEquals(6. / 3, agg.getMAE(), 0.);
        assertEquals(14. / 3, agg.getMSE(), 0.);
        assertEquals(14., agg.getRss(), 0.);
        assertEquals(2.6, agg.ysVariance(), 0.1);
        assertEquals(8.0, agg.ysRss(), 0.1);
    }

    /**
     *
     */
    @Test
    public void testMerge() {
        RegressionMetricStatsAggregator agg1 = new RegressionMetricStatsAggregator();
        RegressionMetricStatsAggregator agg2 = new RegressionMetricStatsAggregator();

        agg1.aggregate(mdl, VectorUtils.of(1.).labeled(2.));
        agg2.aggregate(mdl, VectorUtils.of(2.).labeled(4.));
        agg1.aggregate(mdl, VectorUtils.of(3.).labeled(6.));

        RegressionMetricStatsAggregator res = agg1.mergeWith(agg2);
        assertEquals(6. / 3, res.getMAE(), 0.);
        assertEquals(14. / 3, res.getMSE(), 0.);
        assertEquals(14., res.getRss(), 0.);
        assertEquals(2.6, res.ysVariance(), 0.1);
        assertEquals(8.0, res.ysRss(), 0.1);
    }

    /**
     *
     */
    private static IgniteModel<Vector, Double> mdl = new IgniteModel<Vector, Double>() {
        @Override public Double predict(Vector input) {
            return input.get(0);
        }
    };
}

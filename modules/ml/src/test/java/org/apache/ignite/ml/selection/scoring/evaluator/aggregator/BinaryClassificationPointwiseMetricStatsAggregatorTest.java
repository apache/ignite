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
import org.apache.ignite.ml.selection.scoring.evaluator.context.BinaryClassificationEvaluationContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link BinaryClassificationPointwiseMetricStatsAggregator} class.
 */
public class BinaryClassificationPointwiseMetricStatsAggregatorTest {
    /**
     *
     */
    @Test
    public void testAggregate() {
        BinaryClassificationPointwiseMetricStatsAggregator<Double> aggregator = new BinaryClassificationPointwiseMetricStatsAggregator<>();
        assertEquals(null, aggregator.getFalseLabel());
        assertEquals(null, aggregator.getTruthLabel());

        aggregator.initByContext(new BinaryClassificationEvaluationContext<>(0., 1.));
        assertEquals(0., aggregator.getFalseLabel(), 0.);
        assertEquals(1., aggregator.getTruthLabel(), 0.);
        assertEquals(0, aggregator.getTrueNegative());
        assertEquals(0, aggregator.getFalseNegative());
        assertEquals(0, aggregator.getTruePositive());
        assertEquals(0, aggregator.getFalsePositive());

        aggregator.aggregate(mdl, VectorUtils.of(0.).labeled(0.));
        aggregator.aggregate(mdl, VectorUtils.of(1.).labeled(0.));
        aggregator.aggregate(mdl, VectorUtils.of(1.).labeled(1.));
        aggregator.aggregate(mdl, VectorUtils.of(0.).labeled(1.));

        assertEquals(1, aggregator.getTrueNegative());
        assertEquals(1, aggregator.getFalseNegative());
        assertEquals(1, aggregator.getTruePositive());
        assertEquals(1, aggregator.getFalsePositive());
    }

    /**
     *
     */
    @Test
    public void testMerge() {
        BinaryClassificationPointwiseMetricStatsAggregator<Double> agg1 = new BinaryClassificationPointwiseMetricStatsAggregator<>();
        BinaryClassificationPointwiseMetricStatsAggregator<Double> agg2 = new BinaryClassificationPointwiseMetricStatsAggregator<>();

        agg1.initByContext(new BinaryClassificationEvaluationContext<>(0., 1.));
        agg2.initByContext(new BinaryClassificationEvaluationContext<>(0., 1.));

        agg1.aggregate(mdl, VectorUtils.of(0.).labeled(0.));
        agg1.aggregate(mdl, VectorUtils.of(1.).labeled(0.));
        agg2.aggregate(mdl, VectorUtils.of(1.).labeled(1.));
        agg2.aggregate(mdl, VectorUtils.of(0.).labeled(1.));

        BinaryClassificationPointwiseMetricStatsAggregator<Double> res = agg1.mergeWith(agg2);
        assertEquals(1, res.getTrueNegative());
        assertEquals(1, res.getFalseNegative());
        assertEquals(1, res.getTruePositive());
        assertEquals(1, res.getFalsePositive());
    }

    /**
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMergeInequalAggreagators() {
        BinaryClassificationPointwiseMetricStatsAggregator<Double> agg1 = new BinaryClassificationPointwiseMetricStatsAggregator<>();
        BinaryClassificationPointwiseMetricStatsAggregator<Double> agg2 = new BinaryClassificationPointwiseMetricStatsAggregator<>();

        agg1.initByContext(new BinaryClassificationEvaluationContext<>(0., 0.));
        agg2.initByContext(new BinaryClassificationEvaluationContext<>(1., 1.));
        agg1.mergeWith(agg2);
    }

    /**
     *
     */
    private IgniteModel<Vector, Double> mdl = new IgniteModel<Vector, Double>() {
        @Override public Double predict(Vector input) {
            return input.get(0) == 0.0 ? 0. : 1.;
        }
    };
}

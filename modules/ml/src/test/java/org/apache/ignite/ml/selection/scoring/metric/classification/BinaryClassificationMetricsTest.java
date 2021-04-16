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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.evaluator.EvaluationResult;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for binary classification metrics.
 */
public class BinaryClassificationMetricsTest {
    /**
     *
     */
    @Test
    public void testCalculation() {
        Map<Vector, Double> xorset = new HashMap<Vector, Double>() {{
            put(VectorUtils.of(0., 0.), 0.);
            put(VectorUtils.of(0., 1.), 1.);
            put(VectorUtils.of(1., 0.), 1.);
            put(VectorUtils.of(1., 1.), 0.);
        }};

        IgniteModel<Vector, Double> xorFunction = v -> {
            if (Math.abs(v.get(0) - v.get(1)) < 0.01)
                return 0.;
            else
                return 1.;
        };

        IgniteModel<Vector, Double> andFunction = v -> {
            if (Math.abs(v.get(0) - v.get(1)) < 0.01 && v.get(0) > 0)
                return 1.;
            else
                return 0.;
        };

        IgniteModel<Vector, Double> orFunction = v -> {
            if (v.get(0) > 0 || v.get(1) > 0)
                return 1.;
            else
                return 0.;
        };

        EvaluationResult xorResult = Evaluator.evaluateBinaryClassification(xorset, xorFunction, Vector::labeled);
        assertEquals(1., xorResult.get(MetricName.ACCURACY), 0.01);
        assertEquals(1., xorResult.get(MetricName.PRECISION), 0.01);
        assertEquals(1., xorResult.get(MetricName.RECALL), 0.01);
        assertEquals(1., xorResult.get(MetricName.F_MEASURE), 0.01);

        EvaluationResult andResult = Evaluator.evaluateBinaryClassification(xorset, andFunction, Vector::labeled);
        assertEquals(0.25, andResult.get(MetricName.ACCURACY), 0.01);
        assertEquals(0., andResult.get(MetricName.PRECISION), 0.01); // there is no TP
        assertEquals(0., andResult.get(MetricName.RECALL), 0.01); // there is no TP
        assertEquals(Double.NaN, andResult.get(MetricName.F_MEASURE), 0.01); // // there is no TP and zero in denominator

        EvaluationResult orResult = Evaluator.evaluateBinaryClassification(xorset, orFunction, Vector::labeled);
        assertEquals(0.75, orResult.get(MetricName.ACCURACY), 0.01);
        assertEquals(0.66, orResult.get(MetricName.PRECISION), 0.01); // there is no TP
        assertEquals(1., orResult.get(MetricName.RECALL), 0.01); // there is no TP
        assertEquals(0.8, orResult.get(MetricName.F_MEASURE), 0.01); // // there is no TP and zero in denominator
    }
}

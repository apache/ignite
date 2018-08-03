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

package org.apache.ignite.ml.composition.predictionsaggregator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class WeightedPredictionsAggregatorTest {
    /** */
    @Test
    public void testApply1() {
        WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(new double[] {});
        assertEquals(0.0, aggregator.apply(new double[] {}), 0.001);
    }

    /** */
    @Test
    public void testApply2() {
        WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(new double[] {1.0, 0.5, 0.25});
        assertEquals(3.0, aggregator.apply(new double[] {1.0, 2.0, 4.0}), 0.001);
    }

    /** Non-equal weight vector and predictions case */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArguments() {
        WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(new double[] {1.0, 0.5, 0.25});
        aggregator.apply(new double[] { });
    }
}

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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BinaryClassificationMetrics}.
 */
public class BinaryClassificationMetricsValuesTest {
    /** */
    @Test
    public void testDefaultBehaviour() {
        BinaryClassificationMetricValues metricValues = new BinaryClassificationMetricValues(10, 10, 5, 5, 0.5);

        assertEquals(10, metricValues.tp(), 1e-2);
        assertEquals(10, metricValues.tn(), 1e-2);
        assertEquals(5, metricValues.fn(), 1e-2);
        assertEquals(5, metricValues.fp(), 1e-2);
        assertEquals(0.66, metricValues.accuracy(), 1e-2);
        assertEquals(0.66, metricValues.balancedAccuracy(), 1e-2);
        assertEquals(0.66, metricValues.f1Score(), 1e-2);
        assertEquals(0.33, metricValues.fallOut(), 1e-2);
        assertEquals(0.33, metricValues.fdr(), 1e-2);
        assertEquals(0.33, metricValues.missRate(), 1e-2);
        assertEquals(0.66, metricValues.npv(), 1e-2);
        assertEquals(0.66, metricValues.precision(), 1e-2);
        assertEquals(0.66, metricValues.recall(), 1e-2);
        assertEquals(0.66, metricValues.specificity(), 1e-2);
        assertEquals(0.5, metricValues.rocauc(), 1e-2);
    }
}

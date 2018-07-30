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

package org.apache.ignite.ml.tree.randomforest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.composition.ModelOnFeaturesSubspace;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeConditionalNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RandomForestRegressionTrainerTest {
    /**
     * Number of parts to be tested.
     */
    private static final int[] partsToBeTested = new int[] {1, 2, 3, 4, 5, 7};

    /**
     * Number of partitions.
     */
    @Parameterized.Parameter
    public int parts;

    @Parameterized.Parameters(name = "Data divided on {0} partitions")
    public static Iterable<Integer[]> data() {
        List<Integer[]> res = new ArrayList<>();
        for (int part : partsToBeTested)
            res.add(new Integer[] {part});

        return res;
    }

    /** */
    @Test
    public void testFit() {
        int sampleSize = 1000;
        Map<Double, double[]> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(x1 * x2 + x3 * x4, new double[] {x1, x2, x3, x4});
        }

        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(4, 3, 5, 0.3, 4, 0.1);

        ModelsComposition mdl = trainer.fit(sample, parts, (k, v) -> VectorUtils.of(v), (k, v) -> k);

        mdl.getModels().forEach(m -> {
            assertTrue(m instanceof ModelOnFeaturesSubspace);
            assertTrue(((ModelOnFeaturesSubspace) m).getMdl() instanceof DecisionTreeConditionalNode);
        });

        assertTrue(mdl.getPredictionsAggregator() instanceof MeanValuePredictionsAggregator);
        assertEquals(5, mdl.getModels().size());
    }
}

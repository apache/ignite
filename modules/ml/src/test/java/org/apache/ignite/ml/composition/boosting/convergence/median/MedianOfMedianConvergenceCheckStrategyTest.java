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

package org.apache.ignite.ml.composition.boosting.convergence.median;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategy;
import org.apache.ignite.ml.dataset.impl.local.LocalDataset;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.DecisionTreeDataBuilder;
import org.junit.Assert;
import org.junit.Test;

/** */
public class MedianOfMedianConvergenceCheckStrategyTest {
    /** */
    @Test
    public void testComputing() {
        Map<double[], Double> data = new HashMap<>();
        for(int i = 0; i < 10; i ++)
            data.put(new double[]{i, i + 1}, (double)(2 * (i + 1)));
        data.put(new double[]{10, 11}, 10000.0); //median less sensitive to anomalies in data

        LocalDatasetBuilder<double[], Double> datasetBuilder = new LocalDatasetBuilder<>(data, 1);

        MedianOfMedianConvergenceCheckStrategyFactory factory = new MedianOfMedianConvergenceCheckStrategyFactory(0.1);
        ModelsComposition notConvergedModel = new ModelsComposition(Collections.emptyList(), null) {
            @Override public Double apply(Vector features) {
                return 2.1 * features.get(0);
            }
        };

        ModelsComposition convergedModel = new ModelsComposition(Collections.emptyList(), null) {
            @Override public Double apply(Vector features) {
                return 2 * (features.get(0) + 1);
            }
        };

        IgniteBiFunction<double[], Double, Vector> fExtr = (x, y) -> VectorUtils.of(x);
        IgniteBiFunction<double[], Double, Double> lbExtr = (x, y) -> y;
        ConvergenceCheckStrategy<double[], Double> stgy = factory.create(data.size(),
            x -> x,
            (size, validAnswer, mdlAnswer) -> mdlAnswer - validAnswer,
            datasetBuilder, fExtr, lbExtr
        );

        double error = stgy.computeError(VectorUtils.of(1, 2), 4.0, notConvergedModel);
        Assert.assertEquals(1.9, error, 0.01);
        Assert.assertFalse(stgy.isConverged(notConvergedModel));
        Assert.assertTrue(stgy.isConverged(convergedModel));

        try(LocalDataset<EmptyContext, DecisionTreeData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(), new DecisionTreeDataBuilder<>(fExtr, lbExtr))) {

            double onDSError = stgy.computeMeanErrorOnDataset(dataset, notConvergedModel);
            Assert.assertEquals(1.6, onDSError, 0.01);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

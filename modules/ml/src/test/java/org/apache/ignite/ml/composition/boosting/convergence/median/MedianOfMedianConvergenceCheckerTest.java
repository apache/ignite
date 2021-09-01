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

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDataset;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Assert;
import org.junit.Test;

/** */
public class MedianOfMedianConvergenceCheckerTest extends ConvergenceCheckerTest {
    /** */
    @Test
    public void testConvergenceChecking() {
        data.put(666, VectorUtils.of(10, 11).labeled(100000.0));
        LocalDatasetBuilder<Integer, LabeledVector<Double>> datasetBuilder = new LocalDatasetBuilder<>(data, 1);

        ConvergenceChecker<Integer, LabeledVector<Double>> checker = createChecker(
            new MedianOfMedianConvergenceCheckerFactory(0.1), datasetBuilder);

        double error = checker.computeError(VectorUtils.of(1, 2), 4.0, notConvergedMdl);
        Assert.assertEquals(1.9, error, 0.01);

        LearningEnvironmentBuilder envBuilder = TestUtils.testEnvBuilder();

        Assert.assertFalse(checker.isConverged(envBuilder, datasetBuilder, notConvergedMdl));
        Assert.assertTrue(checker.isConverged(envBuilder, datasetBuilder, convergedMdl));

        try (LocalDataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            envBuilder,
            new EmptyContextBuilder<>(), new FeatureMatrixWithLabelsOnHeapDataBuilder<>(vectorizer),
            TestUtils.testEnvBuilder().buildForTrainer())) {

            double onDSError = checker.computeMeanErrorOnDataset(dataset, notConvergedMdl);
            Assert.assertEquals(1.6, onDSError, 0.01);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

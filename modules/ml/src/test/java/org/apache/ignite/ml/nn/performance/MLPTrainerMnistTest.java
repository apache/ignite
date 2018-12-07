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

package org.apache.ignite.ml.nn.performance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.util.MnistUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests {@link MLPTrainer} on the MNIST dataset using locally stored data.
 */
public class MLPTrainerMnistTest {
    /** Tests on the MNIST dataset. */
    @Test
    public void testMNIST() throws IOException {
        int featCnt = 28 * 28;
        int hiddenNeuronsCnt = 100;

        Map<Integer, MnistUtils.MnistLabeledImage> trainingSet = new HashMap<>();

        int i = 0;
        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTrainingSet(60_000))
            trainingSet.put(i++, e);

        MLPArchitecture arch = new MLPArchitecture(featCnt).
            withAddedLayer(hiddenNeuronsCnt, true, Activators.SIGMOID).
            withAddedLayer(10, false, Activators.SIGMOID);

        MLPTrainer<?> trainer = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate::sum,
                RPropParameterUpdate::avg
            ),
            200,
            2000,
            10,
            123L
        );

        System.out.println("Start training...");
        long start = System.currentTimeMillis();
        MultilayerPerceptron mdl = trainer.fit(
            trainingSet,
            1,
            (k, v) -> VectorUtils.of(v.getPixels()),
            (k, v) -> VectorUtils.num2Vec(v.getLabel(), 10).getStorage().data()
        );
        System.out.println("Training completed in " + (System.currentTimeMillis() - start) + "ms");

        int correctAnswers = 0;
        int incorrectAnswers = 0;

        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTestSet(10_000)) {
            Matrix input = new DenseMatrix(new double[][]{e.getPixels()});
            Matrix outputMatrix = mdl.apply(input);

            int predicted = (int) VectorUtils.vec2Num(outputMatrix.getRow(0));

            if (predicted == e.getLabel())
                correctAnswers++;
            else
                incorrectAnswers++;
        }

        double accuracy = 1.0 * correctAnswers / (correctAnswers + incorrectAnswers);
        assertTrue("Accuracy should be >= 80% (not " + accuracy * 100 + "%)", accuracy >= 0.8);
    }
}

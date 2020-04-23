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

package org.apache.ignite.ml.regressions.linear;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LinearRegressionSGDTrainer}.
 */
public class LinearRegressionSGDTrainerTest extends TrainerTest {
    /**
     * Tests {@code fit()} method on a simple small dataset.
     */
    @Test
    public void testSmallDataFit() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[]{-1.0915526, 1.81983527, -0.91409478, 0.70890712, -24.55724107});
        data.put(1, new double[]{-0.61072904, 0.37545517, 0.21705352, 0.09516495, -26.57226867});
        data.put(2, new double[]{0.05485406, 0.88219898, -0.80584547, 0.94668307, 61.80919728});
        data.put(3, new double[]{-0.24835094, -0.34000053, -1.69984651, -1.45902635, -161.65525991});
        data.put(4, new double[]{0.63675392, 0.31675535, 0.38837437, -1.1221971, -14.46432611});
        data.put(5, new double[]{0.14194017, 2.18158997, -0.28397346, -0.62090588, -3.2122197});
        data.put(6, new double[]{-0.53487507, 1.4454797, 0.21570443, -0.54161422, -46.5469012});
        data.put(7, new double[]{-1.58812173, -0.73216803, -2.15670676, -1.03195988, -247.23559889});
        data.put(8, new double[]{0.20702671, 0.92864654, 0.32721202, -0.09047503, 31.61484949});
        data.put(9, new double[]{-0.37890345, -0.04846179, -0.84122753, -1.14667474, -124.92598583});

        LinearRegressionSGDTrainer<?> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new RPropUpdateCalculator(),
            RPropParameterUpdate.SUM_LOCAL,
            RPropParameterUpdate.AVG
        ), 100000, 10, 100, 123L);

        LinearRegressionModel mdl = trainer.fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        assertArrayEquals(
            new double[]{72.26948107, 15.95144674, 24.07403921, 66.73038781},
            mdl.getWeights().getStorage().data(),
            1e-1
        );

        assertEquals(2.8421709430404007e-14, mdl.getIntercept(), 1e-1);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[]{-1.0915526, 1.81983527, -0.91409478, 0.70890712, -24.55724107});
        data.put(1, new double[]{-0.61072904, 0.37545517, 0.21705352, 0.09516495, -26.57226867});
        data.put(2, new double[]{0.05485406, 0.88219898, -0.80584547, 0.94668307, 61.80919728});
        data.put(3, new double[]{-0.24835094, -0.34000053, -1.69984651, -1.45902635, -161.65525991});
        data.put(4, new double[]{0.63675392, 0.31675535, 0.38837437, -1.1221971, -14.46432611});
        data.put(5, new double[]{0.14194017, 2.18158997, -0.28397346, -0.62090588, -3.2122197});
        data.put(6, new double[]{-0.53487507, 1.4454797, 0.21570443, -0.54161422, -46.5469012});
        data.put(7, new double[]{-1.58812173, -0.73216803, -2.15670676, -1.03195988, -247.23559889});
        data.put(8, new double[]{0.20702671, 0.92864654, 0.32721202, -0.09047503, 31.61484949});
        data.put(9, new double[]{-0.37890345, -0.04846179, -0.84122753, -1.14667474, -124.92598583});

        LinearRegressionSGDTrainer<?> trainer = new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new RPropUpdateCalculator(),
            RPropParameterUpdate.SUM_LOCAL,
            RPropParameterUpdate.AVG
        ), 100000, 10, 100, 0L);

        LinearRegressionModel originalMdl = trainer.withSeed(0).fit(
            data, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        LinearRegressionModel updatedOnSameDS = trainer.withSeed(0).update(
            originalMdl,
            data,
            parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        LinearRegressionModel updatedOnEmptyDS = trainer.withSeed(0).update(
            originalMdl,
            new HashMap<>(),
            parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );

        assertArrayEquals(
            originalMdl.getWeights().getStorage().data(),
            updatedOnSameDS.getWeights().getStorage().data(),
            1.0
        );

        assertEquals(originalMdl.getIntercept(), updatedOnSameDS.getIntercept(), 1.0);

        assertArrayEquals(
            originalMdl.getWeights().getStorage().data(),
            updatedOnEmptyDS.getWeights().getStorage().data(),
            1e-1
        );

        assertEquals(originalMdl.getIntercept(), updatedOnEmptyDS.getIntercept(), 1e-1);
    }
}

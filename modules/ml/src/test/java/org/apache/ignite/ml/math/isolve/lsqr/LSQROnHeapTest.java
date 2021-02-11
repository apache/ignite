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

package org.apache.ignite.ml.math.isolve.lsqr;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.developer.PatchedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LSQROnHeap}.
 */
public class LSQROnHeapTest extends TrainerTest {
    /** Tests solving simple linear system. */
    @Test
    public void testSolveLinearSystem() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(0, VectorUtils.of(3.0, 2.0, -1.0, 1.0));
        data.put(1, VectorUtils.of(2.0, -2.0, 4.0, -2.0));
        data.put(2, VectorUtils.of(-1.0, 0.5, -1.0, 0.0));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.LAST);

        IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func = lv -> new LabeledVector<>(lv.features(), new double[]{lv.label()});

        Preprocessor<Integer, Vector> prerocessor = new PatchedPreprocessor<>(func, vectorizer);

        LSQROnHeap<Integer, Vector> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            TestUtils.testEnvBuilder(),
            new SimpleLabeledDatasetDataBuilder<>(prerocessor),
            TestUtils.testEnvBuilder().buildForTrainer()
        );

        LSQRResult res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false, null);

        assertEquals(3, res.getIterations());
        assertEquals(1, res.getIsstop());
        assertEquals(7.240617907140957E-14, res.getR1norm(), 0.0001);
        assertEquals(7.240617907140957E-14, res.getR2norm(), 0.0001);
        assertEquals(6.344288770224759, res.getAnorm(), 0.0001);
        assertEquals(40.540617492419464, res.getAcond(), 0.0001);
        assertEquals(3.4072322214704627E-13, res.getArnorm(), 0.0001);
        assertEquals(3.000000000000001, res.getXnorm(), 0.0001);
        assertArrayEquals(new double[]{0.0, 0.0, 0.0}, res.getVar(), 1e-6);
        assertArrayEquals(new double[]{1, -2, -2}, res.getX(), 1e-6);
        assertTrue(!res.toString().isEmpty());
    }

    /** Tests solving simple linear system with specified x0. */
    @Test
    public void testSolveLinearSystemWithX0() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(0, VectorUtils.of(3.0, 2.0, -1.0, 1.0));
        data.put(1, VectorUtils.of(2.0, -2.0, 4.0, -2.0));
        data.put(2, VectorUtils.of(-1.0, 0.5, -1.0, 0.0));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.LAST);

        IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func = lv -> new LabeledVector<>(lv.features(), new double[]{lv.label()});

        Preprocessor<Integer, Vector> prerocessor = new PatchedPreprocessor<>(func, vectorizer);

        LSQROnHeap<Integer, Vector> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            TestUtils.testEnvBuilder(),
            new SimpleLabeledDatasetDataBuilder<>(prerocessor),
            TestUtils.testEnvBuilder().buildForTrainer()
        );

        LSQRResult res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false,
            new double[] {999, 999, 999});

        assertEquals(3, res.getIterations());

        assertArrayEquals(new double[]{1, -2, -2}, res.getX(), 1e-6);
    }

    /** Tests solving least squares problem. */
    @Test
    public void testSolveLeastSquares() throws Exception {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(0, VectorUtils.of(-1.0915526, 1.81983527, -0.91409478, 0.70890712, -24.55724107));
        data.put(1, VectorUtils.of(-0.61072904, 0.37545517, 0.21705352, 0.09516495, -26.57226867));
        data.put(2, VectorUtils.of(0.05485406, 0.88219898, -0.80584547, 0.94668307, 61.80919728));
        data.put(3, VectorUtils.of(-0.24835094, -0.34000053, -1.69984651, -1.45902635, -161.65525991));
        data.put(4, VectorUtils.of(0.63675392, 0.31675535, 0.38837437, -1.1221971, -14.46432611));
        data.put(5, VectorUtils.of(0.14194017, 2.18158997, -0.28397346, -0.62090588, -3.2122197));
        data.put(6, VectorUtils.of(-0.53487507, 1.4454797, 0.21570443, -0.54161422, -46.5469012));
        data.put(7, VectorUtils.of(-1.58812173, -0.73216803, -2.15670676, -1.03195988, -247.23559889));
        data.put(8, VectorUtils.of(0.20702671, 0.92864654, 0.32721202, -0.09047503, 31.61484949));
        data.put(9, VectorUtils.of(-0.37890345, -0.04846179, -0.84122753, -1.14667474, -124.92598583));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, 1);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.LAST);

        IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func = lv -> new LabeledVector<>(lv.features(), new double[]{lv.label()});

        Preprocessor<Integer, Vector> prerocessor = new PatchedPreprocessor<>(func, vectorizer);

        try (LSQROnHeap<Integer, Vector> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            TestUtils.testEnvBuilder(),
            new SimpleLabeledDatasetDataBuilder<>(prerocessor), TestUtils.testEnvBuilder().buildForTrainer()))
        {
            LSQRResult res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false, null);

            assertEquals(8, res.getIterations());

            assertArrayEquals(new double[]{72.26948107, 15.95144674, 24.07403921, 66.73038781}, res.getX(), 1e-6);
        }
    }
}

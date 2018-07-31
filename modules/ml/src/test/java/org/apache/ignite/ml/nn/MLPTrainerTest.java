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

package org.apache.ignite.ml.nn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.NesterovParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.NesterovUpdateCalculator;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link MLPTrainer} that don't require to start the whole Ignite infrastructure.
 */
@RunWith(Enclosed.class)
public class MLPTrainerTest {
    /**
     * Parameterized tests.
     */
    @RunWith(Parameterized.class)
    public static class ComponentParamTests {
        /** Number of parts to be tested. */
        private static final int[] partsToBeTested = new int[] {1, 2, 3, 4, 5, 7};

        /** Batch sizes to be tested. */
        private static final int[] batchSizesToBeTested = new int[] {1, 2, 3, 4};

        /** Parameters. */
        @Parameterized.Parameters(name = "Data divided on {0} partitions, training with batch size {1}")
        public static Iterable<Integer[]> data() {
            List<Integer[]> res = new ArrayList<>();
            for (int part : partsToBeTested)
                for (int batchSize1 : batchSizesToBeTested)
                    res.add(new Integer[] {part, batchSize1});

            return res;
        }

        /** Number of partitions. */
        @Parameterized.Parameter
        public int parts;

        /** Batch size. */
        @Parameterized.Parameter(1)
        public int batchSize;

        /**
         * Test 'XOR' operation training with {@link SimpleGDUpdateCalculator} updater.
         */
        @Test
        public void testXORSimpleGD() {
            xorTest(new UpdatesStrategy<>(
                new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal,
                SimpleGDParameterUpdate::avg
            ));
        }

        /**
         * Test 'XOR' operation training with {@link RPropUpdateCalculator}.
         */
        @Test
        public void testXORRProp() {
            xorTest(new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate::sumLocal,
                RPropParameterUpdate::avg
            ));
        }

        /**
         * Test 'XOR' operation training with {@link NesterovUpdateCalculator}.
         */
        @Test
        public void testXORNesterov() {
            xorTest(new UpdatesStrategy<>(
                new NesterovUpdateCalculator<MultilayerPerceptron>(0.1, 0.7),
                NesterovParameterUpdate::sum,
                NesterovParameterUpdate::avg
            ));
        }

        /**
         * Common method for testing 'XOR' with various updaters.
         * @param updatesStgy Update strategy.
         * @param <P> Updater parameters type.
         */
        private <P extends Serializable> void xorTest(UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy) {
            Map<Integer, double[][]> xorData = new HashMap<>();
            xorData.put(0, new double[][]{{0.0, 0.0}, {0.0}});
            xorData.put(1, new double[][]{{0.0, 1.0}, {1.0}});
            xorData.put(2, new double[][]{{1.0, 0.0}, {1.0}});
            xorData.put(3, new double[][]{{1.0, 1.0}, {0.0}});

            MLPArchitecture arch = new MLPArchitecture(2).
                withAddedLayer(10, true, Activators.RELU).
                withAddedLayer(1, false, Activators.SIGMOID);

            MLPTrainer<P> trainer = new MLPTrainer<>(
                arch,
                LossFunctions.MSE,
                updatesStgy,
                3000,
                batchSize,
                50,
                123L
            );

            MultilayerPerceptron mlp = trainer.fit(
                xorData,
                parts,
                (k, v) -> VectorUtils.of(v[0]),
                (k, v) -> v[1]
            );

            Matrix predict = mlp.apply(new DenseMatrix(new double[][]{
                {0.0, 0.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {1.0, 1.0}
            }));

            TestUtils.checkIsInEpsilonNeighbourhood(new DenseVector(new double[]{0.0}), predict.getRow(0), 1E-1);
        }
    }

    /**
     * Non-parameterized tests.
     */
    public static class ComponentSingleTests {
        /** Data. */
        private double[] data;

        /** Initialization. */
        @Before
        public void init() {
            data = new double[10];
            for (int i = 0; i < 10; i++)
                data[i] = i;
        }

        /** */
        @Test
        public void testBatchWithSingleColumnAndSingleRow() {
            double[] res = MLPTrainer.batch(data, new int[]{1}, 10);

            TestUtils.assertEquals(new double[]{1.0}, res, 1e-12);
        }

        /** */
        @Test
        public void testBatchWithMultiColumnAndSingleRow() {
            double[] res = MLPTrainer.batch(data, new int[]{1}, 5);

            TestUtils.assertEquals(new double[]{1.0, 6.0}, res, 1e-12);
        }

        /** */
        @Test
        public void testBatchWithMultiColumnAndMultiRow() {
            double[] res = MLPTrainer.batch(data, new int[]{1, 3}, 5);

            TestUtils.assertEquals(new double[]{1.0, 3.0, 6.0, 8.0}, res, 1e-12);
        }
    }
}

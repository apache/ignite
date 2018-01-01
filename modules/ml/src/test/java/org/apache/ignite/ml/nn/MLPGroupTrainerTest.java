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

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.nn.trainers.distributed.MLPGroupUpdateTrainer;
import org.apache.ignite.ml.nn.updaters.RPropParameterUpdate;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test group trainer.
 */
public class MLPGroupTrainerTest extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 3;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test training of 'xor' by {@link MLPGroupUpdateTrainer}.
     */
    public void testXOR() {
        int samplesCnt = 1000;

        Matrix xorInputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        Matrix xorOutputs = new DenseLocalOnHeapMatrix(new double[][] {{0.0}, {1.0}, {1.0}, {0.0}},
            StorageConstants.ROW_STORAGE_MODE).transpose();

        MLPArchitecture conf = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        IgniteCache<Integer, LabeledVector<Vector, Vector>> cache = LabeledVectorsCache.createNew(ignite);
        String cacheName = cache.getName();
        Random rnd = new Random(12345L);

        try (IgniteDataStreamer<Integer, LabeledVector<Vector, Vector>> streamer =
                 ignite.dataStreamer(cacheName)) {
            streamer.perNodeBufferSize(10000);

            for (int i = 0; i < samplesCnt; i++) {
                int col = Math.abs(rnd.nextInt()) % 4;
                streamer.addData(i, new LabeledVector<>(xorInputs.getCol(col), xorOutputs.getCol(col)));
            }
        }

        int totalCnt = 100;
        int failCnt = 0;
        double maxFailRatio = 0.2;
        MLPGroupUpdateTrainer<RPropParameterUpdate> trainer = MLPGroupUpdateTrainer.getDefault(ignite).
            withSyncRate(3).
            withTolerance(0.001).
            withMaxGlobalSteps(1000);

        for (int i = 0; i < totalCnt; i++) {

            MLPGroupUpdateTrainerCacheInput trainerInput = new MLPGroupUpdateTrainerCacheInput(conf,
                new RandomInitializer(rnd), 6, cache, 4);

            MultilayerPerceptron mlp = trainer.train(trainerInput);

            Matrix predict = mlp.apply(xorInputs);

            Tracer.showAscii(predict);

            X.println(xorOutputs.getRow(0).minus(predict.getRow(0)).kNorm(2) + "");

            failCnt += TestUtils.checkIsInEpsilonNeighbourhoodBoolean(xorOutputs.getRow(0), predict.getRow(0), 5E-1) ? 0 : 1;
        }

        double failRatio = (double)failCnt / totalCnt;

        System.out.println("Fail percentage: " + (failRatio * 100) + "%.");

        assertTrue(failRatio < maxFailRatio);
    }
}

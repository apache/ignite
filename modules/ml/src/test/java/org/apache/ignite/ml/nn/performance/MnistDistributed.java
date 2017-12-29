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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.LabeledVectorsCache;
import org.apache.ignite.ml.nn.MLPGroupUpdateTrainerCacheInput;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.trainers.distributed.MLPGroupUpdateTrainer;
import org.apache.ignite.ml.nn.updaters.RPropParameterUpdate;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.ml.nn.performance.MnistMLPTestUtil.createDataset;
import static org.apache.ignite.ml.nn.performance.MnistMLPTestUtil.loadMnist;

/**
 * Various benchmarks for hand runs.
 */
public class MnistDistributed extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 4;

    /** Features count in MNIST. */
    private static final int FEATURES_CNT = 28 * 28;

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

    @Test
    public void testMNISTDistributed() throws IOException {
        int samplesCnt = 60_000;
        int hiddenNeuronsCnt = 100;

        IgniteBiTuple<Stream<DenseLocalOnHeapVector>, Stream<DenseLocalOnHeapVector>> trainingAndTest = loadMnist(samplesCnt);

        // Load training mnist part into a cache.
        Stream<DenseLocalOnHeapVector> trainingMnist = trainingAndTest.get1();
        List<DenseLocalOnHeapVector> trainingMnistLst = trainingMnist.collect(Collectors.toList());

        IgniteCache<Integer, LabeledVector<Vector, Vector>> labeledVectorsCache = LabeledVectorsCache.createNew(ignite);
        loadIntoCache(trainingMnistLst, labeledVectorsCache);

        MLPGroupUpdateTrainer<RPropParameterUpdate> trainer = MLPGroupUpdateTrainer.getDefault(ignite);

        MLPArchitecture arch = new MLPArchitecture(FEATURES_CNT).
            withAddedLayer(hiddenNeuronsCnt, true, Activators.SIGMOID).
            withAddedLayer(10, false, Activators.SIGMOID);

        MultilayerPerceptron mdl = trainer.train(new MLPGroupUpdateTrainerCacheInput<>(arch, 8, labeledVectorsCache, 2000));

        IgniteBiTuple<Matrix, Matrix> testDs = createDataset(trainingAndTest.get2(), 10_000, FEATURES_CNT);

        Vector predicted = mdl.apply(testDs.get1()).foldColumns(VectorUtils::vec2Num);
        Vector truth = testDs.get2().foldColumns(VectorUtils::vec2Num);

        Tracer.showAscii(truth);
        Tracer.showAscii(predicted);
    }

    /**
     * Load MNIST into cache.
     *
     * @param trainingMnistLst List with mnist data.
     * @param labeledVectorsCache Cache to load MNIST into.
     */
    private void loadIntoCache(List<DenseLocalOnHeapVector> trainingMnistLst,
        IgniteCache<Integer, LabeledVector<Vector, Vector>> labeledVectorsCache) {
        String cacheName = labeledVectorsCache.getName();

        try (IgniteDataStreamer<Integer, LabeledVector<Vector, Vector>> streamer =
                 ignite.dataStreamer(cacheName)) {
            int sampleIdx = 0;

            streamer.perNodeBufferSize(10000);

            for (DenseLocalOnHeapVector vector : trainingMnistLst) {
                streamer.addData(sampleIdx, asLabeledVector(vector, FEATURES_CNT));

                if (sampleIdx % 5000 == 0)
                    X.println("Loaded " + sampleIdx + " samples.");

                sampleIdx++;
            }
        }
    }

//    /**
//     * Split vector of size sz into two parts: feature vector of size featsCnt and label vector of size sz - featsCnt.
//     *
//     * @param v Vector to convert.
//     * @param featsCnt Number of features.
//     * @return LabeledVector.
//     */
    public static LabeledVector<Vector, Vector> asLabeledVector(Vector v, int featsCnt) {
        Vector features = VectorUtils.copyPart(v, 0, featsCnt);
        Vector lb = VectorUtils.num2Vec((int)v.get(featsCnt - 1), 10);

        return new LabeledVector<>(features, lb);
    }
}

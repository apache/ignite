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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.math.Tracer;
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
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link MLPTrainer} that require to start the whole Ignite infrastructure.
 */
public class MLPTrainerIntegrationTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Test 'XOR' operation training with {@link SimpleGDUpdateCalculator}.
     */
    @Test
    public void testXORSimpleGD() {
        xorTest(new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.3),
            SimpleGDParameterUpdate.SUM_LOCAL,
            SimpleGDParameterUpdate.AVG
        ));
    }

    /**
     * Test 'XOR' operation training with {@link RPropUpdateCalculator}.
     */
    @Test
    public void testXORRProp() {
        xorTest(new UpdatesStrategy<>(
            new RPropUpdateCalculator(),
            RPropParameterUpdate.SUM_LOCAL,
            RPropParameterUpdate.AVG
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
     *
     * @param updatesStgy Update strategy.
     * @param <P> Updater parameters type.
     */
    private <P extends Serializable> void xorTest(UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy) {
        CacheConfiguration<Integer, LabeledVector<double[]>> xorCacheCfg = new CacheConfiguration<>();
        xorCacheCfg.setName("XorData");
        xorCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 5));
        IgniteCache<Integer, LabeledVector<double[]>> xorCache = ignite.createCache(xorCacheCfg);

        try {
            xorCache.put(0, VectorUtils.of(0.0, 0.0).labeled(new double[] {0.0}));
            xorCache.put(1, VectorUtils.of(0.0, 1.0).labeled(new double[] {1.0}));
            xorCache.put(2, VectorUtils.of(1.0, 0.0).labeled(new double[] {1.0}));
            xorCache.put(3, VectorUtils.of(1.0, 1.0).labeled(new double[] {0.0}));

            MLPArchitecture arch = new MLPArchitecture(2).
                withAddedLayer(10, true, Activators.RELU).
                withAddedLayer(1, false, Activators.SIGMOID);

            MLPTrainer<P> trainer = new MLPTrainer<>(
                arch,
                LossFunctions.MSE,
                updatesStgy,
                2500,
                4,
                50,
                123L
            );

            MultilayerPerceptron mlp = trainer.fit(ignite, xorCache, new LabeledDummyVectorizer<>());
            Matrix predict = mlp.predict(new DenseMatrix(new double[][] {
                {0.0, 0.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {1.0, 1.0}
            }));

            Tracer.showAscii(predict);

            X.println(new DenseVector(new double[] {0.0}).minus(predict.getRow(0)).kNorm(2) + "");

            TestUtils.checkIsInEpsilonNeighbourhood(new DenseVector(new double[] {0.0}), predict.getRow(0), 1E-1);
        }
        finally {
            xorCache.destroy();
        }
    }
}

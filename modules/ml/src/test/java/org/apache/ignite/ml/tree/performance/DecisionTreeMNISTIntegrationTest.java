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

package org.apache.ignite.ml.tree.performance;

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.performance.MnistMLPTestUtil;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.impurity.util.SimpleStepFunctionCompressor;
import org.apache.ignite.ml.util.MnistUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests {@link DecisionTreeClassificationTrainer} on the MNIST dataset that require to start the whole Ignite
 * infrastructure. For manual run.
 */
public class DecisionTreeMNISTIntegrationTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** Tests on the MNIST dataset. For manual run. */
    public void testMNIST() throws IOException {
        CacheConfiguration<Integer, MnistUtils.MnistLabeledImage> trainingSetCacheCfg = new CacheConfiguration<>();
        trainingSetCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        trainingSetCacheCfg.setName("MNIST_TRAINING_SET");

        IgniteCache<Integer, MnistUtils.MnistLabeledImage> trainingSet = ignite.createCache(trainingSetCacheCfg);

        int i = 0;
        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTrainingSet(60_000))
            trainingSet.put(i++, e);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(
            8,
            0,
            new SimpleStepFunctionCompressor<>());

        DecisionTreeNode mdl = trainer.fit(
            ignite,
            trainingSet,
            (k, v) -> VectorUtils.of(v.getPixels()),
            (k, v) -> (double) v.getLabel()
        );

        int correctAnswers = 0;
        int incorrectAnswers = 0;

        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTestSet(10_000)) {
            double res = mdl.apply(new DenseVector(e.getPixels()));

            if (res == e.getLabel())
                correctAnswers++;
            else
                incorrectAnswers++;
        }

        double accuracy = 1.0 * correctAnswers / (correctAnswers + incorrectAnswers);

        assertTrue(accuracy > 0.8);
    }
}

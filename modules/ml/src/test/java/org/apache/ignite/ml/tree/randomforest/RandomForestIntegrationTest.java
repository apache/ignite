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

package org.apache.ignite.ml.tree.randomforest;

import java.util.ArrayList;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link RandomForestTrainer}.
 */
public class RandomForestIntegrationTest extends GridCommonAbstractTest {
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
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    @Test
    public void testFit() {
        int size = 100;

        CacheConfiguration<Integer, double[]> trainingSetCacheCfg = new CacheConfiguration<>();
        trainingSetCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        trainingSetCacheCfg.setName("TRAINING_SET");

        IgniteCache<Integer, double[]> data = ignite.createCache(trainingSetCacheCfg);

        Random rnd = new Random(0);
        for (int i = 0; i < size; i++) {
            double x = rnd.nextDouble() - 0.5;
            data.put(i, new double[] {x, x > 0 ? 1 : 0});
        }

        ArrayList<FeatureMeta> meta = new ArrayList<>();
        meta.add(new FeatureMeta("", 0, false));
        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(meta)
            .withAmountOfTrees(5)
            .withFeaturesCountSelectionStrgy(x -> 2);

        ModelsComposition mdl = trainer.fit(ignite, data, new DoubleArrayVectorizer<Integer>().labeled(1));

        assertTrue(mdl.getPredictionsAggregator() instanceof MeanValuePredictionsAggregator);
        assertEquals(5, mdl.getModels().size());
    }
}

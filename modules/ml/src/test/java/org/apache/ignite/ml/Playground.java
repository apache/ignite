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

package org.apache.ignite.ml;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.selection.cv.CrossValidationScoreCalculator;
import org.apache.ignite.ml.selection.score.AccuracyCalculator;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class Playground extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 4;

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    public void testTest() {
        CacheConfiguration<Integer, double[]> cacheConfig = new CacheConfiguration<>();
        cacheConfig.setAffinity(new RendezvousAffinityFunction(false, 5));
        cacheConfig.setName("TEST_CACHE");

        IgniteCache<Integer, double[]> cache = ignite.createCache(cacheConfig);

        for (int i = 0; i < 100; i++)
            cache.put(i, new double[]{i, i > 50 ? 1 : -1});

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(10, 0);


        CrossValidationScoreCalculator<DecisionTreeNode, Double, Integer, double[]> scoreCalculator = new CrossValidationScoreCalculator<>();

        double[] scores = scoreCalculator.score(
            trainer,
            ignite,
            cache,
            (k, v) -> true,
            (k, v) -> Arrays.copyOf(v, v.length - 1),
            (k, v) -> v[v.length - 1],
            new AccuracyCalculator<>(),
            new SHA256UniformMapper<>(),
            4
        );

        System.out.println("Scored : " + Arrays.toString(scores));
    }
}

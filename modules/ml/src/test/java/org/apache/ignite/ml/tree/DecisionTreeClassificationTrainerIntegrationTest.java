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

package org.apache.ignite.ml.tree;

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link DecisionTreeClassificationTrainer} that require to start the whole Ignite infrastructure.
 */
public class DecisionTreeClassificationTrainerIntegrationTest extends GridCommonAbstractTest {
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

    /** */
    public void testFit() {
        int size = 100;

        CacheConfiguration<Integer, double[]> trainingSetCacheCfg = new CacheConfiguration<>();
        trainingSetCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        trainingSetCacheCfg.setName("TRAINING_SET");

        IgniteCache<Integer, double[]> data = ignite.createCache(trainingSetCacheCfg);

        Random rnd = new Random(0);
        for (int i = 0; i < size; i++) {
            double x = rnd.nextDouble() - 0.5;
            data.put(i, new double[]{x, x > 0 ? 1 : 0});
        }

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        DecisionTreeNode tree = trainer.fit(
            ignite,
            data,
            (k, v) -> VectorUtils.of(Arrays.copyOf(v, v.length - 1)),
            (k, v) -> v[v.length - 1]
        );

        assertTrue(tree instanceof DecisionTreeConditionalNode);

        DecisionTreeConditionalNode node = (DecisionTreeConditionalNode) tree;

        assertEquals(0, node.getThreshold(), 1e-3);

        assertTrue(node.getThenNode() instanceof DecisionTreeLeafNode);
        assertTrue(node.getElseNode() instanceof DecisionTreeLeafNode);

        DecisionTreeLeafNode thenNode = (DecisionTreeLeafNode) node.getThenNode();
        DecisionTreeLeafNode elseNode = (DecisionTreeLeafNode) node.getElseNode();

        assertEquals(1, thenNode.getVal(), 1e-10);
        assertEquals(0, elseNode.getVal(), 1e-10);
    }
}

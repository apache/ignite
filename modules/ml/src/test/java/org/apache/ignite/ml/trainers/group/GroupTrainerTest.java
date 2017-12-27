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

package org.apache.ignite.ml.trainers.group;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test of {@link GroupTrainer}.
 */
public class GroupTrainerTest extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 4;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * Default constructor.
     */
    public GroupTrainerTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
        TestGroupTrainingCache.getOrCreate(ignite).removeAll();
        TestGroupTrainingSecondCache.getOrCreate(ignite).removeAll();
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

    /** */
    public void testGroupTrainer() {
        TestGroupTrainer trainer = new TestGroupTrainer(ignite);

        int limit = 5;
        int eachNumCnt = 3;
        int iterCnt = 2;

        ConstModel<Integer> mdl = trainer.train(new SimpleGroupTrainerInput(limit, eachNumCnt, iterCnt));
        int locRes = computeLocally(limit, eachNumCnt, iterCnt);
        assertEquals(locRes, (int)mdl.apply(10));
    }

    /** */
    private int computeLocally(int limit, int eachNumCnt, int iterCnt) {
        Map<GroupTrainerCacheKey<Double>, Integer> m = new HashMap<>();

        for (int i = 0; i < limit; i++) {
            for (int j = 0; j < eachNumCnt; j++)
                m.put(new GroupTrainerCacheKey<>(i, (double)j, null), i);
        }

        for (int i = 0; i < iterCnt; i++)
            for (GroupTrainerCacheKey<Double> key : m.keySet())
                m.compute(key, (key1, integer) -> integer * integer);

        return m.values().stream().filter(x -> x % 2 == 0).mapToInt(i -> i).sum();
    }
}

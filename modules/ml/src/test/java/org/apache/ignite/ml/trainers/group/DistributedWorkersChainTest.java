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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.Chains;
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/** */
public class DistributedWorkersChainTest extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 3;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * Default constructor.
     */
    public DistributedWorkersChainTest() {
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
    public void testId() {
        ComputationsChain<TestLocalContext, Double, Integer, Integer, Integer> chain = Chains.create();

        UUID trainingUUID = UUID.randomUUID();
        Integer res = chain.process(1, new GroupTrainingContext<>(new TestLocalContext(0, trainingUUID), TestGroupTrainingCache.getOrCreate(ignite), ignite));

        Assert.assertEquals(1L, (long)res);
    }

    /** */
    public void testSimpleLocal() {
        ComputationsChain<TestLocalContext, Double, Integer, Integer, Integer> chain = Chains.create();

        IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache = TestGroupTrainingCache.getOrCreate(ignite);
        int init = 1;
        int initLocCtxData = 0;
        UUID trainingUUID = UUID.randomUUID();
        TestLocalContext locCtx = new TestLocalContext(initLocCtxData, trainingUUID);

        Integer res = chain.
            thenLocally((prev, lc) -> prev + 1).
            process(init, new GroupTrainingContext<>(locCtx, cache, ignite));

        Assert.assertEquals(init + 1, (long)res);
        Assert.assertEquals(initLocCtxData, locCtx.data());
    }

    /** */
    public void testChainLocal() {
        ComputationsChain<TestLocalContext, Double, Integer, Integer, Integer> chain = Chains.create();

        IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache = TestGroupTrainingCache.getOrCreate(ignite);
        int init = 1;
        int initLocCtxData = 0;
        UUID trainingUUID = UUID.randomUUID();
        TestLocalContext locCtx = new TestLocalContext(initLocCtxData, trainingUUID);

        Integer res = chain.
            thenLocally((prev, lc) -> prev + 1).
            thenLocally((prev, lc) -> prev * 5).
            process(init, new GroupTrainingContext<>(locCtx, cache, ignite));

        Assert.assertEquals((init + 1) * 5, (long)res);
        Assert.assertEquals(initLocCtxData, locCtx.data());
    }

    /** */
    public void testChangeLocalContext() {
        ComputationsChain<TestLocalContext, Double, Integer, Integer, Integer> chain = Chains.create();
        IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache = TestGroupTrainingCache.getOrCreate(ignite);
        int init = 1;
        int newData = 10;
        UUID trainingUUID = UUID.randomUUID();
        TestLocalContext locCtx = new TestLocalContext(0, trainingUUID);

        Integer res = chain.
            thenLocally((prev, lc) -> { lc.setData(newData); return prev;}).
            process(init, new GroupTrainingContext<>(locCtx, cache, ignite));

        Assert.assertEquals(newData, locCtx.data());
        Assert.assertEquals(init, res.intValue());
    }

    /** */
    public void testDistributed() {
        ComputationsChain<TestLocalContext, Double, Integer, Integer, Integer> chain = Chains.create();
        IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache = TestGroupTrainingCache.getOrCreate(ignite);
        int init = 1;
        UUID trainingUUID = UUID.randomUUID();
        TestLocalContext locCtx = new TestLocalContext(0, trainingUUID);

        Map<GroupTrainerCacheKey<Double>, Integer> m = new HashMap<>();
        m.put(new GroupTrainerCacheKey<>(0L, 1.0, trainingUUID), 1);
        m.put(new GroupTrainerCacheKey<>(1L, 2.0, trainingUUID), 2);
        m.put(new GroupTrainerCacheKey<>(2L, 3.0, trainingUUID), 3);
        m.put(new GroupTrainerCacheKey<>(3L, 4.0, trainingUUID), 4);

        Stream<GroupTrainerCacheKey<Double>> keys = m.keySet().stream();

        cache.putAll(m);

        IgniteBiFunction<Integer, TestLocalContext, IgniteSupplier<Stream<GroupTrainerCacheKey<Double>>>> function = (o, l) -> () -> keys;
        IgniteFunction<List<Integer>, Integer> max = ints -> ints.stream().mapToInt(x -> x).max().orElse(Integer.MIN_VALUE);

        Integer res = chain.
            thenDistributedForEntries((integer, context) -> () -> null, this::readAndIncrement, function, max).
            process(init, new GroupTrainingContext<>(locCtx, cache, ignite));

        int localMax = m.values().stream().max(Comparator.comparingInt(i -> i)).orElse(Integer.MIN_VALUE);

        assertEquals((long)localMax, (long)res);

        for (GroupTrainerCacheKey<Double> key : m.keySet())
            m.compute(key, (k, v) -> v + 1);

        assertMapEqualsCache(m, cache);
    }

    /** */
    private ResultAndUpdates<Integer> readAndIncrement(EntryAndContext<Double, Integer, Void> ec) {
        Integer val = ec.entry().getValue();

        ResultAndUpdates<Integer> res = ResultAndUpdates.of(val);
        res.updateCache(TestGroupTrainingCache.getOrCreate(Ignition.localIgnite()), ec.entry().getKey(), val + 1);

        return res;
    }

    /** */
    private <K, V> void assertMapEqualsCache(Map<K, V> m, IgniteCache<K, V> cache) {
        assertEquals(m.size(), cache.size());

        for (K k : m.keySet())
            assertEquals(m.get(k), cache.get(k));
    }
}

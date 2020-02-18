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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Abstract test with utility methods for interruption testing.
 */
@RunWith(Parameterized.class)
public abstract class PartitionReconciliationInterruptionTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Corrupted keys count. */
    protected static final int BROKEN_KEYS_CNT = 500;

    /** Cache atomicity mode. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    /** Cache atomicity mode. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    /** Node to node id. */
    protected Map<Integer, String> nodeToNodeId = new HashMap<>();

    /** Batch size. */
    protected int batchSize = 1;

    /**
     *
     */
    @Parameterized.Parameters(name = "atomicity = {0}, persistence = {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            params.add(new Object[] {atomicityMode, true});
            params.add(new Object[] {atomicityMode, false});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistence)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        ccfg.setBackups(NODES_CNT - 1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++)
            nodeToNodeId.put(i, grid(i).localNode().id().toString());

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        nodeToNodeId.clear();
    }

    /**
     *
     */
    protected void assertErrorMsg(ReconciliationResult res, int nodeId, String errorMsg) {
        String nodeIdStr = nodeToNodeId.get(nodeId);

        for (String error : res.errors()) {
            if (error.startsWith(nodeIdStr) && error.contains(errorMsg))
                return;
        }

        fail("Expected message [msg=" + errorMsg + "] not found for node: " + nodeIdStr);
    }

    /**
     *
     */
    protected void assertErrorMsgLeastOne(ReconciliationResult res, String errorMsg) {
        for (String error : res.errors()) {
            if (error.contains(errorMsg))
                return;
        }

        fail("Expected message [msg=" + errorMsg + "] not found");
    }
}

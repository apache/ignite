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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.BreakRebuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.StopRebuildIndexConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.addCacheRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.nodeName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;

/**
 * Class for testing rebuilding index resumes.
 */
public class ResumeRebuildIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int cnt) throws Exception {
        IgniteEx n = super.startGrid(cnt);

        n.cluster().state(ACTIVE);

        return n;
    }

    @Test
    public void test0() throws Exception {
        IgniteEx n = prepareCluster(100);

        stopAllGrids();
        deleteIndexBin(n.name());

        BreakRebuildIndexConsumer breakRebuildIdxConsumer = new BreakRebuildIndexConsumer(
            getTestTimeout(),
            (c, r) -> c.visitCnt.get() >= 10
        );

        IndexProcessor.idxRebuildCls = IndexesRebuildTaskEx.class;
        addCacheRowConsumer(nodeName(n), DEFAULT_CACHE_NAME, breakRebuildIdxConsumer);

        n = startGrid(0);

        breakRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut0 = indexRebuildFuture(n, CU.cacheId(DEFAULT_CACHE_NAME));
        assertFalse(idxRebFut0.isDone());

        breakRebuildIdxConsumer.finishRebuildIdxFut.onDone();
        assertThrows(log, () -> idxRebFut0.get(getTestTimeout()), Throwable.class, null);

        assertTrue(cacheMetrics0(n, DEFAULT_CACHE_NAME).getIndexRebuildKeysProcessed() < 100);
        assertTrue(breakRebuildIdxConsumer.visitCnt.get() < 100);

        stopAllGrids();

        StopRebuildIndexConsumer stopRebuildIdxConsumer = new StopRebuildIndexConsumer(getTestTimeout());

        IndexProcessor.idxRebuildCls = IndexesRebuildTaskEx.class;
        addCacheRowConsumer(nodeName(n), DEFAULT_CACHE_NAME, stopRebuildIdxConsumer);

        n = startGrid(0);

        stopRebuildIdxConsumer.startRebuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut1 = indexRebuildFuture(n, CU.cacheId(DEFAULT_CACHE_NAME));
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishRebuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        assertEquals(100, cacheMetrics0(n, DEFAULT_CACHE_NAME).getIndexRebuildKeysProcessed());
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Prepare cluster for test.
     *
     * @param keys Key count.
     * @return Coordinator.
     * @throws Exception If failed.
     */
    private IgniteEx prepareCluster(int keys) throws Exception {
        IgniteEx n = startGrid(0);

        for (int i = 0; i < keys; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "name_" + i));

        return n;
    }
}

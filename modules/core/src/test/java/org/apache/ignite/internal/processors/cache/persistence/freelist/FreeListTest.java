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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList.IGNITE_PAGES_LIST_STRIPES_PER_BUCKET;
import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 * Test freelists.
 */
@WithSystemProperty(key = IGNITE_PAGES_LIST_STRIPES_PER_BUCKET, value = "1")
public class FreeListTest extends GridCommonAbstractTest {
    private static final int KEYS_COUNT = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        int pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(pageSize * 3L * KEYS_COUNT));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** */
    @Test
    public void testConcurrentUpdatesAndRemoves() throws Exception {
        IgniteEx ignite = startGrid(0);

        int partCnt = 1;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        Random random = new Random(3793);

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, getRecord(random));

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture<Long> updateFuture = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                try {
                    cache.put(random.nextInt(KEYS_COUNT), getRecord(random));
                }
                catch (Exception ex) {
                    stop.set(true);
                    ignite.log().error("can't put", ex);
                    throw ex;
                }
            }
        }, 11, "update");

        runMultiThreaded(() -> {
            for (int i = 0; i < KEYS_COUNT * 1000 && !stop.get(); i++) {
                if (i % 50000 == 0)
                    ignite.log().info(String.format("%s: i=%d; size=%d", Thread.currentThread().getName(), i, cache.size()));

                int del = random.nextInt(KEYS_COUNT);

                cache.remove(del);

//                for (int j = del; j < del + random.nextInt(5); j++)
//                    cache.remove(j);
            }
        },1,"remove");

        stop.set(true);

        updateFuture.get();

//        runMultiThreaded(() -> {
//            for (int i = 0; i < KEYS_COUNT * 100; i++) {
//                if (i % 50000 == 0)
//                    ignite.log().info(String.format("%s: i=%d; size=%d", Thread.currentThread().getName(), i, cache.size()));
//
//                cache.put(random.nextInt(KEYS_COUNT), getRecord(random));
//
//                int del = random.nextInt(KEYS_COUNT);
//
//                for (int j = del; j < del + random.nextInt(5); j++)
//                    cache.remove(j);
//            }
//        },24,"update-remove");
    }

    /** B+Tree is corrupted, msg=Runtime failure on cursor iteration
     * Caused by: class org.apache.ignite.IgniteException: B+Tree is corrupted [groupId=1544803905, pageIds=[844420635206029], msg=Runtime failure on cursor iteration]
	at org.apache.ignite.internal.util.lang.GridIteratorAdapter.hasNext(GridIteratorAdapter.java:48)
	at org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl.clearCache(IgniteCacheOffheapManagerImpl.java:466)
	at org.apache.ignite.internal.processors.cache.GridCacheClearAllRunnable.run(GridCacheClearAllRunnable.java:84)
	at org.apache.ignite.internal.processors.cache.GridCacheAdapter.clearLocally(GridCacheAdapter.java:1046)
	at org.apache.ignite.internal.processors.cache.GridCacheProxyImpl.clearLocally(GridCacheProxyImpl.java:941)
	at org.apache.ignite.internal.processors.cache.GridCacheAdapter$GlobalClearAllJob.localExecute(GridCacheAdapter.java:5157)
	at org.apache.ignite.internal.processors.cache.GridCacheAdapter$TopologyVersionAwareJob.execute(GridCacheAdapter.java:6231)
	at org.apache.ignite.internal.processors.job.GridJobWorker$1.call(GridJobWorker.java:628)
	at org.apache.ignite.internal.util.IgniteUtils.wrapThreadLoader(IgniteUtils.java:5800)
	at org.apache.ignite.internal.processors.job.GridJobWorker.execute0(GridJobWorker.java:622)
	at org.apache.ignite.internal.processors.job.GridJobWorker.body(GridJobWorker.java:547)
	at org.apache.ignite.internal.util.worker.GridWorker.run(GridWorker.java:125)
	at org.apache.ignite.internal.processors.job.GridJobProcessor.runSync(GridJobProcessor.java:1475)
	at org.apache.ignite.internal.processors.job.GridJobProcessor.processJobExecuteRequest(GridJobProcessor.java:1388)
	at org.apache.ignite.internal.processors.task.GridTaskWorker.sendRequest(GridTaskWorker.java:1441)
	at org.apache.ignite.internal.processors.task.GridTaskWorker.processMappedJobs(GridTaskWorker.java:670)
	at org.apache.ignite.internal.processors.task.GridTaskWorker.body(GridTaskWorker.java:534)
	... 19 more
Caused by: class org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException: B+Tree is corrupted [groupId=1544803905, pageIds=[844420635206029], msg=Runtime failure on cursor iteration]
	at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.corruptedTreeException(BPlusTree.java:6057)
	at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$AbstractForwardCursor.nextPage(BPlusTree.java:5624)
	at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$ForwardCursor.next(BPlusTree.java:5793)
	at org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl$4.onHasNext(IgniteCacheOffheapManagerImpl.java:748)
	at org.apache.ignite.internal.util.GridCloseableIteratorAdapter.hasNextX(GridCloseableIteratorAdapter.java:56)
	at org.apache.ignite.internal.util.lang.GridIteratorAdapter.hasNext(GridIteratorAdapter.java:45)
	... 35 more
Caused by: java.lang.IllegalStateException: Unknown page type: 13 pageId: 0002ffff0000a18d
	at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.io(BPlusTree.java:5326)
	at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$AbstractForwardCursor.nextPage(BPlusTree.java:5609)
	... 39 more
     */
    @Test
    public void testConcurrentUpdatesAndClear() throws Exception {
        IgniteEx ignite = startGrid(0);

        int partCnt = 1;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        Random random = new Random(3793);

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, getRecord(random));

        AtomicBoolean stop = new AtomicBoolean(false);

        runAsync(() -> {
            while(!stop.get()) {
                cache.put(random.nextInt(KEYS_COUNT), getRecord(random));
            }
        });

        cache.clear();

        stop.set(true);
    }

    /** */
    private byte[] getRecord(Random random) {
//        if (random.nextInt(5) == 3)
//            return new byte[random.nextInt(20, 300)];
//        else
            return new byte[random.nextInt(3000, 15000)];
    }
}

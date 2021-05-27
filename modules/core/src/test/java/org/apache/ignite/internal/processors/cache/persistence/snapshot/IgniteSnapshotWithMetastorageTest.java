/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorker;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;

/**
 * Cluster-wide snapshot with distributed metastorage test.
 */
public class IgniteSnapshotWithMetastorageTest extends AbstractSnapshotSelfTest {
    /** */
    private static final String SNAPSHOT_PREFIX = "SNAPSHOT_PREFIX_";

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithMetastorage() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);
        startClientGrid();

        ignite.context().distributedMetastorage().write("key", "value");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertEquals("value", snp.context().distributedMetastorage().read("key"));
    }

    /** @throws Exception If fails. */
    @Test
    public void testMetastorageUpdateDuringSnapshot() throws Exception {
        AtomicInteger keyCnt = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<?> updFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && !stop.get()) {
                try {
                    ignite.context().distributedMetastorage().write(SNAPSHOT_PREFIX + keyCnt.getAndIncrement(), "value");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, 3, "dms-updater");

        DmsDataWriterWorker worker = GridTestUtils.getFieldValue(ignite.context().distributedMetastorage(),
            DistributedMetaStorageImpl.class, "worker");
        LinkedBlockingQueue<RunnableFuture<?>> queue = GridTestUtils.getFieldValue(worker, DmsDataWriterWorker.class,
            "updateQueue");

        RunnableFuture<?> testTask = new FutureTask<>(() -> {
            U.await(latch);

            return null;
        });

        queue.offer(testTask);

        assertTrue(GridTestUtils.waitForCondition(() -> queue.size() > 10, getTestTimeout()));

        ignite.context().cache().context().exchange()
            .registerExchangeAwareComponent(new PartitionsExchangeAware() {
                /** {@inheritDoc} */
                @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                    latch.countDown();
                }
            });

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stop.set(true);
        updFut.get();

        stopAllGrids();

        Function<IgniteConfiguration, String> pathProv = cfg -> resolveSnapshotWorkDirectory(cfg).getAbsolutePath();
        Set<String> keySet0 = new TreeSet<>();
        Set<String> keySet1 = new TreeSet<>();

        IgniteEx snp0 = startGridsFromSnapshot(Collections.singleton(0), pathProv, SNAPSHOT_NAME, false);
        snp0.context().distributedMetastorage().iterate(SNAPSHOT_PREFIX, (key, val) -> keySet0.add(key));

        stopGrid(0);

        IgniteEx snp1 = startGridsFromSnapshot(Collections.singleton(1), pathProv, SNAPSHOT_NAME, false);
        snp1.context().distributedMetastorage().iterate(SNAPSHOT_PREFIX, (key, val) -> keySet1.add(key));

        assertEquals("Keys must be the same on all nodes", keySet0, keySet1);
    }

    /** @throws Exception If fails. */
    @Test
    public void testMetastorageUpdateOnSnapshotFail() throws Exception {
        AtomicInteger keyCnt = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        Set<String> writtenKeys = new TreeSet<>();

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<?> updFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && !stop.get()) {
                try {
                    String key = SNAPSHOT_PREFIX + keyCnt.getAndIncrement();

                    ignite.context().distributedMetastorage().write(key, "value");

                    writtenKeys.add(key);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, 3, "dms-updater");

        ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database())
            .addCheckpointListener(new CheckpointListener() {
                @Override public void onMarkCheckpointBegin(Context ctx) {
                    if (ctx.progress().reason().contains(SNAPSHOT_NAME)) {
                        Map<String, SnapshotFutureTask> locMap =
                            GridTestUtils.getFieldValue(snp(ignite), IgniteSnapshotManager.class, "locSnpTasks");

                        // Fail the snapshot task with an exception, all metastorage keys must be successfully continued.
                        locMap.get(SNAPSHOT_NAME).acceptException(new IgniteCheckedException("Test exception"));
                    }
                }

                @Override public void onCheckpointBegin(Context ctx) {
                }

                @Override public void beforeCheckpointBegin(Context ctx) {
                }
            });

        IgniteFuture<?> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        GridTestUtils.assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, "Test exception");

        stop.set(true);

        // Load future must complete without exceptions, all metastorage keys must be written.
        updFut.get();

        Set<String> readedKeys = new TreeSet<>();

        ignite.context().distributedMetastorage().iterate(SNAPSHOT_PREFIX, (key, val) -> readedKeys.add(key));

        assertEquals("Not all metastorage keys have been written", writtenKeys, readedKeys);
    }
}

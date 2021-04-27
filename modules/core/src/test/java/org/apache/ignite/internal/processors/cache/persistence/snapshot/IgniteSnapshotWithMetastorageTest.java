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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

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

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stop.set(true);
        updFut.get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        Set<String> allKeys = new HashSet<>();
        snp.context().distributedMetastorage().iterate(SNAPSHOT_PREFIX, (key, val) -> allKeys.add(key));

        for (Ignite ig : G.allGrids()) {
            // Iterator reads keys from the node heap map.
            ((IgniteEx)ig).context().distributedMetastorage()
                .iterate(SNAPSHOT_PREFIX, new BiConsumer<String, Serializable>() {
                @Override public void accept(String key, Serializable value) {
                    try {
                        assertTrue("Keys must be the same on all nodes [key=" + key + ", all=" + allKeys + ']',
                            allKeys.contains(key));
                    }
                    catch (Throwable t) {
                        fail("Exception reading metastorage: " + t.getMessage());
                    }
                }
            });
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testMetastorageUpdateOnSnapshotFail() throws Exception {
        AtomicInteger keyCnt = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<?> updFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && !stop.get()) {
                try {
                    ignite.context().distributedMetastorage()
                        .write(SNAPSHOT_PREFIX + keyCnt.getAndIncrement(), "value");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, 3, "dms-updater");

        GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        db.addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                if (ctx.progress().reason().contains("Snapshot")) {
                    Map<String, SnapshotFutureTask> locMap = GridTestUtils.getFieldValue(snp(ignite), IgniteSnapshotManager.class,
                        "locSnpTasks");

                    // Fail the snapshot task with an exception, all metastorage keys must be successfully continued.
                    locMap.get(SNAPSHOT_NAME).acceptException(new IgniteCheckedException("Test exception"));
                }
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {}

            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {}
        });

        IgniteFuture<?> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        GridTestUtils.assertThrowsAnyCause(log,
            fut::get,
            IgniteCheckedException.class,
            "Test exception");

        stop.set(true);

        // Load future must complete without exceptions, all metastorage keys must be written.
        updFut.get();

        Set<Integer> allKeys = IntStream.range(0, keyCnt.get()).boxed().collect(Collectors.toSet());

        ignite.context().distributedMetastorage().iterate(SNAPSHOT_PREFIX, new BiConsumer<String, Serializable>() {
            @Override public void accept(String key, Serializable val) {
                try {
                    assertTrue(allKeys.remove(Integer.parseInt(key.replace(SNAPSHOT_PREFIX, ""))));
                }
                catch (Throwable t) {
                    fail("Exception reading metastorage: " + t.getMessage());
                }
            }
        });

        assertTrue("Not all metastorage keys have been written: " + allKeys, allKeys.isEmpty());
    }
}

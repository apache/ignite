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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Cluster-wide snapshot with distributed metastorage test.
 */
public class IgniteSnapshotWithMetastorageTest extends AbstractSnapshotSelfTest {
    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithMetastorage() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);
        startClientGrid();

        ignite.context().distributedMetastorage().write("key", "value");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        assertEquals("value", snp.context().distributedMetastorage().read("key"));
    }

    /** @throws Exception If fails. */
    @Test
    public void testMetastorageUpdateDuringSnapshot() throws Exception {
        String prefix = "SNAPSHOT_PREFIX_";
        AtomicInteger keyCnt = new AtomicInteger();
        AtomicBoolean done = new AtomicBoolean();
        AtomicBoolean stop = new AtomicBoolean();

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<?> updFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && !stop.get()) {
                try {
                    ignite.context().distributedMetastorage().write(prefix + keyCnt.incrementAndGet(), "value");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, 3, "dms-updater");

        GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        int[] marked = new int[1];

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // Save counter under checkpoint write-lock.
                // No distributed metastorage updates should be saved to snapshot after this counter.
                if (done.compareAndSet(false, true))
                    marked[0] = keyCnt.get();
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }
        });

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        assertTrue(GridTestUtils.waitForCondition(() -> keyCnt.get() > marked[0], getTestTimeout()));

        stop.set(true);
        updFut.get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        snp.context().distributedMetastorage().iterate(prefix, new BiConsumer<String, Serializable>() {
            @Override public void accept(String s, Serializable serializable) {
                try {
                    int key = Integer.parseInt(s.replace(prefix, ""));

                    assertTrue(key <= marked[0]);
                }
                catch (Throwable t) {
                    fail("Exception reading metastorage");
                }
            }
        });
    }
}

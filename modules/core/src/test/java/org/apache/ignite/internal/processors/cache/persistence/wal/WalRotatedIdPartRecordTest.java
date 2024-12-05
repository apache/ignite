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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/** */
public class WalRotatedIdPartRecordTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 1000;

    /** */
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    /** */
    AtomicBoolean stop = new AtomicBoolean();

    /** */
    CountDownLatch complete = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setPersistenceEnabled(true)))
                .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
                    .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 60))));

            // Plugin that creates a testing WAL manager.
            cfg.setPluginProviders(new AbstractTestPluginProvider() {
                /** {@inheritDoc} */
                @Override public String name() {
                    return "testPlugin";
                }

                /** {@inheritDoc} */
                @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                    if (IgniteWriteAheadLogManager.class.equals(cls))
                        return (T)new TestFileWriteAheadLogManager(((IgniteEx)ctx.grid()).context(), stop);

                    return null;
                }
            });
        }

        return cfg;
    }

    /** WAL manager that signals once meet the RotatedIdPartRecord with rotatedIdPart less than 0. */
    private static class TestFileWriteAheadLogManager extends FileWriteAheadLogManager {
        /** */
        private final AtomicBoolean stop;

        /** Constructor. */
        public TestFileWriteAheadLogManager(GridKernalContext ctx, AtomicBoolean stop) {
            super(ctx);

            this.stop = stop;
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
            if (rec instanceof RotatedIdPartRecord && ((RotatedIdPartRecord)rec).rotatedIdPart() < 0)
                stop.set(true);

            return super.log(rec);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRotatedIdMoreThan127() throws Exception {
        startGrids(2).cluster().state(ClusterState.ACTIVE);

        IgniteEx cln = startClientGrid(2);

        for (int i = 0; i < KEYS; i++)
            cln.cache(DEFAULT_CACHE_NAME).put(i, rnd.nextInt());

        forceCheckpoint();

        disableCheckpoints(grid(0));
        disableCheckpoints(grid(1));

        IgniteInternalFuture<?> touchFut = runMultiThreadedAsync(() -> {
            int cnt = 0;

            while (!stop.get()) {
                cln.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).forEach((v) -> {});

                cln.log().info(String.format("Query#: %d", cnt++));
            }

            complete.countDown();
        }, 1, "touch");

        boolean completed = complete.await(120, TimeUnit.SECONDS);

        if (!completed)
            stop.set(true);

        touchFut.get(60, TimeUnit.SECONDS);

        stopAllClients(false);

//        grid(0).context().pools().getStripedExecutorService().awaitComplete();
//        grid(1).context().pools().getStripedExecutorService().awaitComplete();

        stopAllGrids();

        assertTrue("rotatedIdPart doesn't become > 127 in 120 seconds", completed);

        startGrids(2);
    }

    /** */
    private void disableCheckpoints(IgniteEx ignite) throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database()).enableCheckpoints(false).get();
    }
}

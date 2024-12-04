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

import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/** */
@RunWith(Parameterized.class)
public class WalRotatedIdPartRecordTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 1000;

    /** */
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode mode;

    /** */
    AtomicBoolean stop = new AtomicBoolean();

    /** */
    CountDownLatch complete = new CountDownLatch(1);

    /** */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection<Object[]> params() {
        Collection<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode cacheMode: CacheAtomicityMode.values())
            params.add(new Object[] { cacheMode });

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setPersistenceEnabled(true)))
                .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(mode)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
                    .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 60))));

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
    public void walRotatedIdPartRecord() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        GridCacheProcessor cacheProc = ignite0.context().cache();
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cacheProc.context().database();
        dbMgr.enableCheckpoints(false).get();

        IgniteEx cln = startClientGrid(2);

        for (int i = 0; i < KEYS; i++)
            cln.cache(DEFAULT_CACHE_NAME).put(i, rnd.nextInt());

        runMultiThreadedAsync(() -> {
            while (!stop.get())
                cln.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();

            complete.countDown();
        }, 1, "touch");

        assertTrue("rotatedIdPart doesn't become > 127 in 120 seconds",
            complete.await(120, TimeUnit.SECONDS));

        stopAllClients(false);

        ignite0.context().pools().getStripedExecutorService().awaitComplete();
        ignite1.context().pools().getStripedExecutorService().awaitComplete();

        stopAllGrids(false);

        ignite0 = startGrids(2);
        ignite0.cluster().state(ClusterState.ACTIVE);
        ignite0.cluster().state(ClusterState.INACTIVE);
    }
}

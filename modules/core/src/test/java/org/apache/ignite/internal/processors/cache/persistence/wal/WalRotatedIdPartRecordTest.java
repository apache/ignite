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
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class WalRotatedIdPartRecordTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 1000;

    /** */
    AtomicBoolean stop = new AtomicBoolean();

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
                    .setAffinity(new RendezvousAffinityFunction().setPartitions(2))
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

            cfg.setMetricsLogFrequency(1000);
        }

        return cfg;
    }

    /** WAL manager that signals once meet the RotatedIdPartRecord with rotatedIdPart more than 127. */
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
            WALPointer res = super.log(rec);

            if (rec instanceof RotatedIdPartRecord) {
                byte rotatedIdPart = ((RotatedIdPartRecord)rec).rotatedIdPart();

                if (Byte.toUnsignedInt(rotatedIdPart) > 127)
                    stop.set(true);
            }

            return res;
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
            cln.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        disableCheckpoints(grid(0));
        disableCheckpoints(grid(1));

        IgniteInternalFuture<?> touchFut = runAsync(() -> {
            while (!stop.get())
                cln.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).forEach((v) -> {});
        }, "touch");

        try {
            touchFut.get(120, TimeUnit.SECONDS);
        }
        catch (Exception ignored) {
            touchFut.cancel();

            fail("rotatedIdPart doesn't become > 127 in 120 seconds");
        }
        finally {
            // Turn off excessive logging of the "Failed to update TTL: class o.a.i.i.NodeStoppingException"
            // during nodes stop. Errors are expected since not all GridCacheTtlUpdateRequest messages are processed yet.
            Configurator.setLevel(GridCacheMapEntry.class.getName(), Level.OFF);
        }

        stopAllGrids();

        // Restore logging.
        Configurator.setLevel(GridCacheMapEntry.class.getName(), Level.ERROR);

        startGrids(2).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    private void disableCheckpoints(IgniteEx ignite) throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database()).enableCheckpoints(false).get();
    }
}

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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent under streaming load.
 */
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setBackups(1);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(100L * 1024L * 1204L);
        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);
        cfg.getDataStorageConfiguration().setCheckpointFrequency(3000);

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testFailsDefaultReceiver() throws Exception {
        doTest(true, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testOkBatchedReceiver() throws Exception {
        doTest(false, DataStreamerCacheUpdaters.batched());
    }

    /** @throws Exception If fails. */
    @Test
    public void testOkIndividualReceiver() throws Exception {
        doTest(false, DataStreamerCacheUpdaters.individual());
    }

    /**  */
    private void doTest(boolean mustFail, @Nullable StreamReceiver<Integer, Integer> receiver) throws Exception {
        int preLoadCnt = 10_000;

        startGridsMultiThreaded(2);

        grid(0).cluster().state(ACTIVE);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        IgniteEx secondarySrv = startGrid(G.allGrids().size());

        IgniteEx client = startClientGrid(G.allGrids().size());

        IgniteSnapshotManager snpMngr = snp(grid(0));

        // We need to check the datastreamer from all node types: baseline server, non-baseline server and client.
        for (Ignite loaderNode : Arrays.asList(grid(1), secondarySrv, client)) {
            U.delete(snpMngr.snapshotLocalDir(SNAPSHOT_NAME, null));

            grid(0).cache(dfltCacheCfg.getName()).clear();

            CountDownLatch loadBeforeSnapshot = new CountDownLatch(preLoadCnt);
            AtomicBoolean stopLoading = new AtomicBoolean(false);

            IgniteInternalFuture<?> loadFut = runLoad(loaderNode, receiver, loadBeforeSnapshot, stopLoading);

            loadBeforeSnapshot.await();

            if (mustFail) {
                String errMsg = "streaming loading with the streamer's property 'alowOverwrite' set to `false`. Such " +
                    "updates doesn't guarantee consistency until finished. The snapshot might not be entirely restored.";

                assertThrows(null, () -> grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(),
                    IgniteException.class, errMsg);
            }
            else
                grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();

            stopLoading.set(true);
            loadFut.get();
        }
    }

    /** */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, StreamReceiver<Integer, Integer> receiver,
        CountDownLatch startSnp, AtomicBoolean stop) {

        grid(0).getOrCreateCache(dfltCacheCfg);

        return GridTestUtils.runMultiThreadedAsync(() -> {
            int idx = 0;

            try (IgniteDataStreamer<Integer, Integer> ds = ldr.dataStreamer(dfltCacheCfg.getName())) {
                if (receiver != null)
                    ds.receiver(receiver);

                while (!stop.get()) {
                    ds.addData(idx, idx);

                    idx++;

                    startSnp.countDown();
                }
            }
            catch (Exception e) {
                while (startSnp.getCount() > 0)
                    startSnp.countDown();

                throw e;
            }
        }, 1, "load-thread");
    }
}

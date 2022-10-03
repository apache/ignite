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
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.jline.internal.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent under streaming load.
 */
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Iterable<Boolean> encryptionParams() {
        return Arrays.asList(false);
    }

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(2);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.getDataStorageConfiguration().setCheckpointFrequency(1000);

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testFailsWithDefaultReceiver() throws Exception {
        doTest(true, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testFailsWithBatchedReceiver() throws Exception {
        doTest(false, DataStreamerCacheUpdaters.batched());
    }

    /** @throws Exception If fails. */
    @Test
    public void testFailsWithIndividualReceiver() throws Exception {
        doTest(false, DataStreamerCacheUpdaters.individual());
    }

    /**  */
    private void doTest(boolean expectFailure, @Nullable StreamReceiver<Integer, Integer> receiver) throws Exception {
        startGrids(3);

        grid(0).cluster().state(ACTIVE);

        CountDownLatch loadBeforeSnd = new CountDownLatch(50_000);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger keyProvider = new AtomicInteger();

        IgniteInternalFuture<?> load = runLoad(receiver, keyProvider, loadBeforeSnd, stop);

        loadBeforeSnd.await();

        if (expectFailure) {
            assertThrows(null, () -> {
                grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();
            }, IgniteException.class, "Streaming should not work while snapshot");
        }
        else
            grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stop.set(true);

        load.get();

        if (!expectFailure) {
            grid(0).cache(dfltCacheCfg.getName()).destroy();

            awaitPartitionMapExchange();

            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get();
        }
    }

    /** */
    private IgniteInternalFuture<?> runLoad(StreamReceiver<Integer, Integer> receiver, AtomicInteger idx,
        CountDownLatch startSnp, AtomicBoolean stop) {
        return GridTestUtils.runMultiThreadedAsync(() -> {
            try (Ignite client = startClientGrid(G.allGrids().size())) {
                try (IgniteDataStreamer<Integer, Integer> ds = client.dataStreamer(dfltCacheCfg.getName())) {
                    if (receiver != null)
                        ds.receiver(receiver);

                    while (!stop.get()) {
                        int i = idx.incrementAndGet();

                        ds.addData(i, i);

                        startSnp.countDown();
                    }
                }
                catch (Exception e) {
                    while (startSnp.getCount() > 0)
                        startSnp.countDown();

                    if (!(e instanceof IllegalStateException && e.getMessage().contains("streamer has been closed.")))
                        throw e;
                }
            }
            catch (Exception e) {
                log.error("Unable to close client.", e);
            }
        }, 1, "load-thread");
    }
}

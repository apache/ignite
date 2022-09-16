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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class IgniteClusterShanpshotStreamerTest  extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Iterable<Boolean> encryptionParams() {
        return Arrays.asList(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

//        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(2 * 1024L * 1024L * 1024L);
//        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);
//        cfg.getDataStorageConfiguration().setWalSegments(4);
//        cfg.getDataStorageConfiguration().setWalSegmentSize(16 * 1024 * 1024);
//        cfg.getDataStorageConfiguration().setMaxWalArchiveSize(128 * 1024 * 1024);
//        cfg.getDataStorageConfiguration().setCheckpointFrequency(1000);
//        cfg.getDataStorageConfiguration().setCheckpointReadLockTimeout(15_000);

//        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotConsistencyWithStreamer() throws Exception {
        int grids = 3;
        int backups = 2;

//        CountDownLatch loadLever = new CountDownLatch(13_403);
        CountDownLatch loadLever = new CountDownLatch(20_000_000);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger idx = new AtomicInteger();
        dfltCacheCfg = null;
        String tableName = "TEST_TBL1";

        startGridsMultiThreaded(grids);
        grid(0).cluster().state(ACTIVE);

//        GridNearAtomicUpdateFuture.TEST_NODE_UID = grid(0).localNode().id();

        IgniteCache<Integer, Integer> cache = grid(0)
            .createCache(new CacheConfiguration<Integer, Integer>("SQL_PUBLIC_" + tableName).setBackups(backups)
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setCacheMode(CacheMode.PARTITIONED)
//                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setBackups(backups)
//                                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
//                            .setGroupName("grp1")
            );

        long n = System.nanoTime();

        runLoad2(tableName, loadLever.getCount());

        n = U.nanosToMillis(System.nanoTime() - n) / 1000;

        System.err.println("TEST | loaded in " + n + " seconds.");

        if(true)
            return;

        IgniteInternalFuture<?> load1 = runLoad(tableName, idx, loadLever, stop);
//        IgniteInternalFuture<?> load2 = runLoad(tableName, idx, loadLever, stop);

        loadLever.await();

        log.info("TEST | createSnapshot.");

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stop.set(true);
        load1.get();
//        load2.get();

        log.info("TEST | stop loading, destroy cache.");

        grid(0).cache("SQL_PUBLIC_" + tableName).destroy();
//        grid(0).cache("cache2").destroy();

        awaitPartitionMapExchange();

        log.info("TEST | restoreSnapshot.");

//        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList("cache2")).get();
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList("SQL_PUBLIC_" + tableName)).get();
//        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList("grp1")).get();

        assertEquals(grid(0).cache("SQL_PUBLIC_" + tableName).get(1),
            grid(1).cache("SQL_PUBLIC_" + tableName).get(1));
    }

    /** */
    private IgniteInternalFuture<?> runLoad(String tblName, AtomicInteger idx, CountDownLatch startSnp, AtomicBoolean stop) {
        return GridTestUtils.runMultiThreadedAsync(() -> {
            String cacheName = "SQL_PUBLIC_" + tblName.toUpperCase();

            try (Ignite client = startClientGrid(2)) {
//                IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);

                try (IgniteDataStreamer<Integer, Integer> ds = client.dataStreamer(cacheName)) {
                    ds.allowOverwrite(false);
//                    ds.allowOverwrite(true);
//                    ds.skipStore(false);

                    while (!stop.get()) {
                        int i = idx.incrementAndGet();

                        ds.addData(i, i);
//                        cache.put(i, new Account(i, i - 1));

//                        batch.put(i, new Account(i, i - 1));
//
//                        if(batch.size() > 99){
//                            cache.putAll(batch);
//
//                            batch.clear();
//                        }

                        startSnp.countDown();

                        Thread.yield();
                    }

//                    cache.putAll(batch);

//                    batch.clear();
                }
                catch (Exception e) {
                    while (startSnp.getCount() > 0)
                        startSnp.countDown();

                    log.error("Datastramer closed with error.", e);

                    // throw new IgniteException("Unable to load.", e);
                }

                log.error("TEST | datastreamer futures left: " + grid(2).context().cache().context().mvcc().dataStreamerFutures().size());
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, "load-thread-" + tblName);
    }

    private void runLoad2(String tblName, long cnt) {
        String cacheName = "SQL_PUBLIC_" + tblName.toUpperCase();

//        Map<Long, Long> batch = new HashMap<>();

        try (Ignite client = startClientGrid(G.allGrids().size())) {
            try (IgniteDataStreamer<Long, Long> ds = client.dataStreamer(cacheName)) {
                ds.allowOverwrite(true);
//                ds.perNodeBufferSize(32);

                for (long i = 0; i < cnt; ++i) {
                    ds.addData(i, i);

//                    batch.put(i, i);
//
//                    if(batch.size() >= 512) {
//                        grid(0).cache(cacheName).putAll(batch);
//
//                        batch = new HashMap<>();
//                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** */
    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000;
    }

    /** */
    private void createTable(Connection conn, String tableName, int backups) throws SQLException {
        conn.prepareStatement("create table " + tableName +
            " (\n" +
            "id int,\n" +
            "name varchar not null,\n" +
            "orgid int not null,\n" +
            "dep int default 0,\n" +
            "primary key (id, orgid)\n" +
            ")\n" +
            "with \"template=partitioned,backups=" + backups + "\";").execute();
    }
}

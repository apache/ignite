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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class IgniteClusterShanpshotStreamerTest  extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Iterable<Boolean> encryptionParams() {
        return Arrays.asList(false);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotConsistencyWithStreamer() throws Exception {
        int grids = 2;
        int backups = grids - 1;
        int loadBeforeSnp = 150_000;

        CountDownLatch loadLever = new CountDownLatch(loadBeforeSnp);
        AtomicBoolean stopLoading = new AtomicBoolean(false);
        dfltCacheCfg = null;
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String tableName = "TEST_TBL1";

        startGrids(grids);
        grid(0).cluster().state(ACTIVE);

        IgniteInternalFuture<?> load1 = runLoad(tableName, true, backups, true, stopLoading, loadLever);

        loadLever.await();

        log.info("TEST | createSnapshot.");

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopLoading.set(true);

        load1.get();

        log.info("TEST | stop loading, destroy cache.");

        grid(0).cache("SQL_PUBLIC_" + tableName).destroy();

        awaitPartitionMapExchange();

        log.info("TEST | restoreSnapshot.");

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, F.asList("SQL_PUBLIC_" + tableName)).get();
    }

    /** */
    private IgniteInternalFuture<?> runLoad(String tblName, boolean useCache, int backups, boolean streaming, AtomicBoolean stop,
        CountDownLatch startSnp) {
        return GridTestUtils.runMultiThreadedAsync(() -> {
            if(useCache) {
                String cacheName = "SQL_PUBLIC_" + tblName.toUpperCase();

                IgniteCache<Integer, Object> cache = grid(0)
                    .createCache(new CacheConfiguration<Integer, Object>(cacheName).setBackups(backups)
                        .setCacheMode(CacheMode.REPLICATED));

                try (IgniteDataStreamer<Integer, Object> ds = grid(0).dataStreamer(cacheName)) {
                    ds.allowOverwrite(false);

                    for (int i = 0; !stop.get(); ++i) {
                        if (streaming)
                            ds.addData(i, new Account(i, i - 1));
                        else
                            cache.put(i, new Account(i, i - 1));

                        startSnp.countDown();
                    }
                }
            } else {
                try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
                    createTable(conn, tblName, backups);

                    try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + tblName +
                        "(id, name, orgid, dep) VALUES(?, ?, ?, ?)")) {

                        if (streaming)
                            conn.prepareStatement("SET STREAMING ON;").execute();

                        int leftLimit = 97; // letter 'a'
                        int rightLimit = 122; // letter'z'
                        int targetStringLength = 15;
                        Random rand = new Random();
//
                        for (int i = 0; !stop.get(); ++i) {
                            int orgid = rand.ints(1, 0, 5).findFirst().getAsInt();

                            String val = rand.ints(leftLimit, rightLimit + 1).limit(targetStringLength)
                                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                                .toString();
                            stmt.setInt(1, i);
                            stmt.setString(2, val);
                            stmt.setInt(3, orgid);
                            stmt.setInt(4, 0);

                            stmt.executeUpdate();

                            if (startSnp.getCount() > 0)
                                startSnp.countDown();

                            Thread.yield();
                        }
                    }
                }
                catch (Exception e) {
                    while (startSnp.getCount() > 0)
                        startSnp.countDown();

                    throw new IgniteException("Unable to load.", e);
                }
            }
        }, 1, "load-thread-" + tblName);
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

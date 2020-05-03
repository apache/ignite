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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.testframework.junits.GridAbstractTest.LOCAL_IP_FINDER;

/**
 * To run load from several JVM's with configured profiling log.
 *
 * Start nodes in parallel run: {@link #startServer()} and {@link #startClient()}.
 * Load will be started when all nodes started ({@link #NODES_COUNT}).
 */
public class LoadTestHelper {
    /** Test load duration. */
    private static final int LOAD_DURATION = 60_000;

    /** Count of nodes (clients and servers) to wait topology for. */
    private static final int NODES_COUNT = 4;

    /** Count load threads. */
    private static final int THREADS_PER_NODE = 4;

    /** Path to log configuration. */
    private static final String LOG_CONFIG = "modules/profiling/config/log4j2-profiling.xml";

    /** */
    private static final String CACHE1 = "cache1_tx_one_backup_full_sync";

    /** */
    private static final String CACHE2 = "cache2_atomic_one_backup_primary_sync";

    /** */
    private static final String CACHE3 = "cache3_atomic_one_backup_async_no_sql";

    /** Stop load flag. */
    private final AtomicBoolean stop = new AtomicBoolean();

    /** Latch to wait all nodes started. */
    private static final String LATCH_NAME = "latch";

    /** Run server. */
    @Test
    public void startServer() throws Exception {
        run(true);
    }

    /** Run server. */
    @Test
    public void startServer2() throws Exception {
        run(true);
    }

    /** Run client. */
    @Test
    public void startClient() throws Exception {
        run(false);
    }

    /** Run client. */
    @Test
    public void startClient2() throws Exception {
        run(false);
    }

    /** @param client {@code True} if strat client. */
    private void run(boolean client) throws Exception {
        try (Ignite node = G.start(getConfiguration(client))) {
            IgniteCountDownLatch latch = node.countDownLatch(LATCH_NAME, NODES_COUNT, true, true);

            latch.countDown();
            latch.await();

            if (client) {
                IgniteInternalFuture fut1 = runCacheOperations(node, true);
                IgniteInternalFuture fut2 = runCacheOperations(node, false);
                IgniteInternalFuture fut3 = runQueries(node);
                IgniteInternalFuture fut4 = runTasks(node);

                U.sleep(LOAD_DURATION);

                stop.set(true);

                fut1.get();
                fut2.get();
                fut3.get();
                fut4.get();
            }
            else
                U.sleep(LOAD_DURATION + 2000);
        }
    }

    /** */
    private IgniteConfiguration getConfiguration(boolean client) throws IgniteCheckedException {
        CacheConfiguration[] ccfg = new CacheConfiguration[]{
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE1)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(CACHE1))),

            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE2)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
                .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(CACHE2))),

            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE3)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
        };

        return new IgniteConfiguration()
            .setGridLogger(new Log4J2Logger(LOG_CONFIG))
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(LOCAL_IP_FINDER))
            .setClientMode(client)
            .setCacheConfiguration(ccfg)
            .setIgniteInstanceName(client ? "client" : "srv" + "-" + new Random().nextInt());
    }

    /** */
    private IgniteInternalFuture runCacheOperations(Ignite ignite, boolean withSql) {
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);
        IgniteCache<Object, Object> cache3 = ignite.cache(CACHE3);

        Set<Integer> keySet = Stream.of(1,2,3,4,5,6,7,8).collect(Collectors.toSet());

        Random rand = new Random();

        return GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                IgniteCache<Object, Object> cache = !withSql ? cache3 : rand.nextBoolean() ? cache2 : cache1;

                int key = rand.nextInt(2048);

                switch (rand.nextInt(8)) {
                    case 0:
                        cache.put(key, key);

                        break;

                    case 1:
                        cache.get(key);

                        break;

                    case 2:
                        cache.remove(key);

                        break;

                    case 3:
                        cache.getAndPut(key, key + 1);

                        break;

                    case 4:
                        cache.getAndRemove(key);

                        break;

                    case 5:
                        cache.invoke(key, (entry, arguments) -> {
                            return null;
                        });

                        break;

                    case 6:
                        cache.getAll(keySet);

                        break;

                    case 7:
                        if (!cache.getName().equals(CACHE1))
                            continue;

                        try (Transaction tx = ignite.transactions().txStart()) {
                            cache.get(key);

                            cache.put(key, key + 1);

                            if (rand.nextInt(100) == 1)
                                tx.rollback();
                            else
                                tx.commit();
                        }

                        break;
                }
            }
        }, THREADS_PER_NODE, "cache-ops-load");
    }

    /** */
    private IgniteInternalFuture runQueries(Ignite ignite) {
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);

        Random rand = new Random();

        return GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                IgniteCache<Object, Object> cache = rand.nextBoolean() ? cache1 : cache2;

                switch (rand.nextInt(4)) {
                    case 0:
                        cache.query(new ScanQuery<>()).getAll();

                        break;

                    case 1:
                        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + cache.getName())
                            .setSchema(cache.getName());

                        cache.query(qry).getAll();

                        break;

                    case 2:
                        SqlFieldsQuery qry0 = new SqlFieldsQuery(
                            "SELECT _key FROM " + cache.getName() + " WHERE _key < ?")
                            .setArgs(new Random().nextInt(2048))
                            .setSchema(cache.getName());

                        cache.query(qry0).getAll();

                        break;

                    case 3:
                        SqlFieldsQuery qry1 = new SqlFieldsQuery(
                            "SELECT _key FROM " + cache.getName() + " WHERE _key <= " +
                                new Random().nextInt(5) + " LIMIT 1")
                            .setSchema(cache.getName());

                        cache.query(qry1).getAll();

                        break;
                }
            }
        }, THREADS_PER_NODE, "sql-load");
    }

    /** */
    private IgniteInternalFuture runTasks(Ignite ignite) {
        Random rand = new Random();

        return GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                switch (rand.nextInt(2)) {
                    case 0:
                        ignite.compute().broadcast(new IgniteRunnable() {
                            @Override public void run() {
                                try {
                                    U.sleep(rand.nextInt(30));
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

                        break;

                    case 1:
                        ignite.compute().call(new IgniteCallable<Object>() {
                            @Override public Object call() throws Exception {
                                try {
                                    U.sleep(rand.nextInt(30));
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    e.printStackTrace();
                                }

                                return true;
                            }
                        });

                        break;
                }
            }
        }, THREADS_PER_NODE, "compute-load");
    }
}

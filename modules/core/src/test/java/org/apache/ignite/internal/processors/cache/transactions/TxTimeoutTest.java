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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxTimeoutTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE_" + name.substring(name.length() - 1));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(3)
        );

        if (client){
            cfg.setConsistentId("Client");

            cfg.setClientMode(client);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(4);

        client = true;

        IgniteEx igClient = startGrid(4);

        igClient.cluster().active(true);

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteCache<Integer, Long> cache = igClient.cache(DEFAULT_CACHE_NAME);

        //int threads = Runtime.getRuntime().availableProcessors();
        int threads = 1;

        int keyId = 0;

        CountDownLatch finishLatch = new CountDownLatch(threads);

        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<Long> f = runMultiThreadedAsync(() -> {
            IgniteTransactions txMgr = igClient.transactions();

            while (!stop.get()) {
                long newVal = cnt.getAndIncrement();

                try (Transaction tx = txMgr.txStart(PESSIMISTIC, REPEATABLE_READ, 5, 1)) {
                    cache.put(keyId, newVal);

                    tx.commit();
                }
                catch (TransactionTimeoutException e) {
                    continue;
                }
                catch (CacheException e) {
                    if (X.hasCause(e, TransactionTimeoutException.class))
                        continue;
                }
            }

            finishLatch.countDown();

        }, threads, "tx-runner");

        runAsync(() -> {
            try {
                int time = 60 * 1000;

                Thread.sleep(time);
            }
            catch (InterruptedException ignore) {
                // Ignore.
            }

            stop.set(true);
        });

        finishLatch.await();

        f.get();

        Set<String> caches = new HashSet<>();

        caches.add(DEFAULT_CACHE_NAME);

        VisorIdleVerifyTaskArg taskArg = new VisorIdleVerifyTaskArg(caches);

        VisorIdleVerifyTaskResult res = igClient.compute().execute(
            VisorIdleVerifyTask.class.getName(),
            new VisorTaskArgument<>(ig.localNode().id(), taskArg, false));

        Map<PartitionKey, List<PartitionHashRecord>> conflicts = res.getConflicts();

        log.info("Current counter value:" + cnt.get());

        Long val = cache.get(keyId);

        log.info("Last commited value:" + val);

        if (!F.isEmpty(conflicts)){
            SB sb = new SB();

            sb.a("\n");

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : conflicts.entrySet()) {
                sb.a(entry.getKey()).a("\n");

                for (PartitionHashRecord rec : entry.getValue())
                    sb.a("\t").a(rec).a("\n");
            }

            System.out.println(sb);

            fail();
        }
    }
}

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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheLockReleaseNodeLeaveTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockRelease() throws Exception {
        startGrids(2);

        final Ignite ignite0 = ignite(0);
        final Ignite ignite1 = ignite(1);

        final Integer key = primaryKey(ignite1.cache(null));

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Lock lock = ignite0.cache(null).lock(key);

                lock.lock();

                return null;
            }
        }, "lock-thread1");

        fut1.get();

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Lock lock = ignite1.cache(null).lock(key);

                log.info("Start lock.");

                lock.lock();

                log.info("Locked.");

                return null;
            }
        }, "lock-thread2");

        U.sleep(1000);

        log.info("Stop node.");

        ignite0.close();

        fut2.get(5, SECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxLockRelease() throws Exception {
        startGrids(2);

        final Ignite ignite0 = ignite(0);
        final Ignite ignite1 = ignite(1);

        final Integer key = primaryKey(ignite1.cache(null));

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                ignite0.cache(null).get(key);

                return null;
            }
        }, "lock-thread1");

        fut1.get();

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    log.info("Start tx lock.");

                    ignite1.cache(null).get(key);

                    log.info("Tx locked key.");

                    tx.commit();
                }

                return null;
            }
        }, "lock-thread2");

        U.sleep(1000);

        log.info("Stop node.");

        ignite0.close();

        fut2.get(5, SECONDS);
    }
}

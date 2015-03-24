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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.IgniteState.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Tests for node failure in transactions.
 */
public abstract class GridCacheNodeFailureAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** */
    private static final Integer KEY = 1;

    /** */
    private static final String VALUE = "test";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid instances. */
    private static final List<Ignite> IGNITEs = new ArrayList<>();

    /**
     * Start grid by default.
     */
    protected GridCacheNodeFailureAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setDeploymentMode(DeploymentMode.SHARED);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            IGNITEs.add(startGrid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        IGNITEs.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            if (Ignition.state(IGNITEs.get(i).name()) == STOPPED) {
                info("Restarting grid: " + i);

                IGNITEs.set(i, startGrid(i));
            }

            assert !jcache(i).isLocalLocked(KEY, false) : "Entry is locked for grid [idx=" + i + ']';
        }
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> GridCache<K, V> cache(int i) {
        return ((IgniteKernal)IGNITEs.get(i)).getCache(null);
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> IgniteCache<K, V> jcache(int i) {
        return IGNITEs.get(i).cache(null);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommitted() throws Throwable {
        // TODO:  GG-7437.
        if (jcache(0).getConfiguration(CacheConfiguration.class).getCacheMode() == CacheMode.REPLICATED)
            return;

        checkTransaction(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Throwable {
        checkTransaction(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializable() throws Throwable {
        checkTransaction(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    private void checkTransaction(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Throwable {
        int idx = RAND.nextInt(GRID_CNT);

        info("Grid will be stopped: " + idx);

        Ignite g = grid(idx);

        Transaction tx = g.transactions().txStart(concurrency, isolation);

        try {
            g.cache(null).put(KEY, VALUE);

            int checkIdx = (idx + 1) % G.allGrids().size();

            info("Check grid index: " + checkIdx);

            IgniteFuture<?> f = waitForLocalEvent(grid(checkIdx).events(), new P1<Event>() {
                @Override public boolean apply(Event e) {
                    info("Received grid event: " + e);

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid(idx);

            f.get();

            U.sleep(getInteger(IGNITE_TX_SALVAGE_TIMEOUT, 3000));

            IgniteCache<Integer, String> checkCache = jcache(checkIdx);

            boolean locked = false;
            Lock lock = checkCache.lock(KEY);

            for (int i = 0; !locked && i < 3; i++) {
                locked = lock.tryLock();

                if (!locked)
                    U.sleep(500);
                else
                    break;
            }

            assert locked : "Failed to lock key on cache [idx=" + checkIdx + ", key=" + KEY + ']';

            lock.unlock();
        }
        catch (IgniteTxOptimisticCheckedException e) {
            U.warn(log, "Optimistic transaction failure (will rollback) [msg=" + e.getMessage() + ", tx=" + tx + ']');

            if (G.state(g.name()) == IgniteState.STARTED)
                tx.rollback();

            assert concurrency == OPTIMISTIC && isolation == SERIALIZABLE;
        }
        catch (Throwable e) {
            error("Transaction failed (will rollback): " + tx, e);

            if (G.state(g.name()) == IgniteState.STARTED)
                tx.rollback();

            throw e;
        }
    }

    /**
     * @throws Exception If check failed.
     */
    public void testLock() throws Exception {
        int idx = 0;

        info("Grid will be stopped: " + idx);

        info("Nodes for key [id=" + grid(idx).affinity(null).mapKeyToPrimaryAndBackups(KEY) +
            ", key=" + KEY + ']');

        IgniteCache<Integer, String> cache = jcache(idx);

        // TODO:  GG-7437.
        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() == CacheMode.REPLICATED)
            return;

        cache.put(KEY, VALUE);

        Lock lock = cache.lock(KEY);

        assert lock.tryLock();

        int checkIdx = 1;

        info("Check grid index: " + checkIdx);

        IgniteCache<Integer, String> checkCache = jcache(checkIdx);

        assert !checkCache.lock(KEY).tryLock();

        IgniteFuture<?> f = waitForLocalEvent(grid(checkIdx).events(), new P1<Event>() {
            @Override public boolean apply(Event e) {
                info("Received grid event: " + e);

                return true;
            }
        }, EVT_NODE_LEFT);

        stopGrid(idx);

        f.get();

        boolean locked = false;
        Lock checkLock = checkCache.lock(KEY);

        for (int i = 0; !locked && i < 3; i++) {
            locked = checkLock.tryLock();

            if (!locked) {
                info("Still not locked...");

                U.sleep(1500);
            }
            else
                break;
        }

        assert locked : "Failed to lock";

        checkLock.unlock();

        assert !checkCache.isLocalLocked(KEY, false);
    }
}

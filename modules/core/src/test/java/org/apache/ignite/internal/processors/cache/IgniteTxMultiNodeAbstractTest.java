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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;


/**
 * Checks basic multi-node transactional operations.
 */
@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions", "PointlessArithmeticExpression"})
public abstract class IgniteTxMultiNodeAbstractTest extends GridCommonAbstractTest {
    /** Debug flag. */
    private static final boolean DEBUG = false;

    /** */
    protected static final int GRID_CNT = 4;

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int RETRIES = 300;

    /** Log frequency. */
    private static final int LOG_FREQ = RETRIES < 100 || DEBUG ? 1 : RETRIES / 5;

    /** Counter key. */
    private static final String CNTR_KEY = "CNTR_KEY";

    /** Removed counter key. */
    private static final String RMVD_CNTR_KEY = "RMVD_CNTR_KEY";

    /** */
    protected static final AtomicInteger cntr = new AtomicInteger();

    /** */
    protected static final AtomicInteger cntrRmvd = new AtomicInteger();

    /** Number of backups for partitioned tests. */
    protected int backups = 2;

     /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = 0;

        cntr.set(0);
    }

    /**
     *
     * @param ignite Grid
     * @param key Key.
     * @return Primary node id.
     */
    @SuppressWarnings("unchecked")
    private static UUID primaryId(Ignite ignite, Object key) {
        Affinity aff = ignite.affinity(null);

        Collection<ClusterNode> affNodes = aff.mapPartitionToPrimaryAndBackups(aff.partition(key));

        ClusterNode first = F.first(affNodes);

        assert first != null;

        return first.id();
    }

    /**
     * @param nodeId Node ID.
     * @param key Key.
     * @return DHT entry.
     */
    @Nullable private static GridCacheEntryEx dhtEntry(UUID nodeId, Object key) {
        Ignite g = G.ignite(nodeId);

        GridDhtCacheAdapter<Object, Integer> dht =
            ((IgniteKernal)g).<Object, Integer>internalCache().context().near().dht();

        return dht.peekEx(key);
    }

    /**
     * @param nodeId Node ID.
     * @param key Key.
     * @return Near entry.
     */
    @Nullable private static GridCacheEntryEx nearEntry(UUID nodeId, Object key) {
        Ignite g = G.ignite(nodeId);

        GridNearCacheAdapter<Object, Integer> near = ((IgniteKernal)g).<Object, Integer>internalCache().context().near();

        return near.peekEx(key);
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param itemKey Item key.
     * @param retry Retry count.
     */
    @SuppressWarnings("unchecked")
    private void onItemNear(boolean putCntr, Ignite ignite, String itemKey, int retry) {
        IgniteCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID itemPrimaryId = primaryId(ignite, itemKey);
        UUID cntrPrimaryId = primaryId(ignite, CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                info("Before near get [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(CNTR_KEY);

            int newVal = cntr + 1;

            if (putCntr) {
                if (DEBUG)
                    info("Before near put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

                cache.put(CNTR_KEY, newVal);
            }

            if (DEBUG)
                info("Before near put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr + ", new=" + newVal +
                    ", nearEntry=" + nearEntry(locId, itemKey) + ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            cache.put(itemKey, newVal);

            if (DEBUG)
                info("After near put item [retry=" + retry + ", key=" + itemKey + ", old=" + cntr + ", new=" + newVal +
                    ", nearEntry=" + nearEntry(locId, itemKey) + ", dhtEntry" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param itemKey Item key.
     * @param retry Retry count.
     */
    @SuppressWarnings("unchecked")
    private void onItemPrimary(boolean putCntr, Ignite ignite, String itemKey, int retry) {
        IgniteCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID itemPrimaryId = primaryId(ignite, itemKey);
        UUID cntrPrimaryId = primaryId(ignite, CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                info("Before item primary get [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(CNTR_KEY);

            int newVal = cntr + 1;

            if (putCntr) {
                if (DEBUG)
                    info("Before item primary put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, CNTR_KEY) : "") + ']');

                cache.put(CNTR_KEY, newVal);
            }

            if (DEBUG)
                info("Before item primary put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            cache.put(itemKey, cntr);

            if (DEBUG)
                info("After item primary put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param retry Retry count.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onRemoveItemQueried(boolean putCntr, Ignite ignite, int retry) throws IgniteCheckedException {
        IgniteCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID cntrPrimaryId = primaryId(ignite, RMVD_CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                ignite.log().info("Before item lock [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(RMVD_CNTR_KEY);

            assert cntr != null : "Received null counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']';

            int newVal = cntr - 1;

            if (putCntr) {
                if (DEBUG)
                    ignite.log().info("Before item put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

                cache.put(RMVD_CNTR_KEY, newVal);
            }

            while (true) {
                SqlQuery<String, Integer> qry =
                    new SqlQuery<>(Integer.class, "_key != 'RMVD_CNTR_KEY' and _val >= 0");

                if (DEBUG)
                    ignite.log().info("Before executing query [retry=" + retry + ", locId=" + locId +
                        ", txId=" + tx.xid() + ']');

                Cache.Entry<String, Integer> entry = F.first(cache.query(qry).getAll());

                if (entry == null) {
                    ignite.log().info("*** Queue is empty.");

                    return;
                }

                String itemKey = entry.getKey();

                UUID itemPrimaryId = primaryId(ignite, itemKey);

                // Lock the item key.
                if (cache.get(itemKey) != null) {
                    if (DEBUG)
                        ignite.log().info("Before item remove [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                            ", nearEntry=" + nearEntry(locId, itemKey) +
                            ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

                    assert cache.remove(itemKey) : "Failed to remove key [locId=" + locId +
                        ", primaryId=" + itemPrimaryId + ", key=" + itemKey + ']';

                    if (DEBUG)
                        info("After item remove item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                            ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                            ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

                    break;
                }
                else
                    cache.remove(itemKey);
            }

            tx.commit();
        }
        catch (Error e) {
            ignite.log().error("Error in test.", e);

            throw e;
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @param retry Retry count.
     */
    @SuppressWarnings("unchecked")
    private void onRemoveItemSimple(boolean putCntr, Ignite ignite, int retry) {
        IgniteCache<String, Integer> cache = ignite.cache(null);

        UUID locId = ignite.cluster().localNode().id();
        UUID cntrPrimaryId = primaryId(ignite, RMVD_CNTR_KEY);

        boolean isCntrPrimary = cntrPrimaryId.equals(locId);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (DEBUG)
                ignite.log().info("Before item lock [retry=" + retry + ", xid=" + tx.xid() + ", node=" + ignite.name() +
                    ", isCntrPrimary=" + isCntrPrimary + ", nearId=" + locId +
                    ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                    (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

            Integer cntr = cache.get(RMVD_CNTR_KEY);

            assert cntr != null : "Received null counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']';

            String itemKey = Integer.toString(cntrRmvd.getAndIncrement());

            Integer val = cache.get(itemKey);

            assert val != null : "Received null val [retry=" + retry + ", cacheSize=" + cache.size() + ']';

            UUID itemPrimaryId = primaryId(ignite, itemKey);

            int newVal = cntr - 1;

            if (putCntr) {
                if (DEBUG)
                    ignite.log().info("Before item put counter [retry=" + retry + ", isCntrPrimary=" + isCntrPrimary +
                        ", cur=" + cntr + ", new=" + newVal + ", nearEntry=" + nearEntry(locId, RMVD_CNTR_KEY) +
                        (isCntrPrimary ? ", dhtEntry=" + dhtEntry(locId, RMVD_CNTR_KEY) : "") + ']');

                cache.put(RMVD_CNTR_KEY, newVal);
            }

            if (DEBUG)
                ignite.log().info("Before item remove item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            assertTrue(cache.remove(itemKey));

            if (DEBUG)
                info("After item put item [retry=" + retry + ", key=" + itemKey + ", cur=" + cntr +
                    ", new=" + newVal + ", nearEntry=" + nearEntry(locId, itemKey) +
                    ", dhtEntry=" + dhtEntry(itemPrimaryId, itemKey) + ']');

            tx.commit();
        }
        catch (Error e) {
            ignite.log().error("Error in test.", e);

            throw e;
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     */
    private void retries(Ignite ignite, boolean putCntr) {
        UUID nodeId = ignite.cluster().localNode().id();

        for (int i = 0; i < RETRIES; i++) {
            int cnt = cntr.getAndIncrement();

            if (DEBUG)
                ignite.log().info("***");
            if (DEBUG || cnt % LOG_FREQ == 0)
                ignite.log().info("*** Iteration #" + i + " ***");
            if (DEBUG)
                ignite.log().info("***");

            String itemKey = nodeId + "-#" + i;

            if (nodeId.equals(primaryId(ignite, itemKey)))
                onItemPrimary(putCntr, ignite, itemKey, i);
            else
                onItemNear(putCntr, ignite, itemKey, i);
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     * @throws IgniteCheckedException If failed.
     */
    private void removeRetriesQueried(Ignite ignite, boolean putCntr) throws IgniteCheckedException {
        for (int i = 0; i < RETRIES; i++) {
            if (DEBUG)
                ignite.log().info("***");

            if (DEBUG || cntrRmvd.getAndIncrement() % LOG_FREQ == 0)
                ignite.log().info("*** Iteration #" + i + " ***");

            if (DEBUG)
                ignite.log().info("***");

            onRemoveItemQueried(putCntr, ignite, i);

            if (i % 50 == 0)
                ((IgniteKernal) ignite).internalCache().context().tm().printMemoryStats();
        }
    }

    /**
     *
     * @param putCntr Put counter to cache.
     * @param ignite Grid.
     */
    private void removeRetriesSimple(Ignite ignite, boolean putCntr) {
        for (int i = 0; i < RETRIES; i++) {
            if (DEBUG)
                ignite.log().info("***");

            if (cntrRmvd.get() % LOG_FREQ == 0 || DEBUG)
                ignite.log().info("*** Iteration #" + i + " ***");

            if (DEBUG)
                ignite.log().info("***");

            onRemoveItemSimple(putCntr, ignite, i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutOneEntryInTx() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            grid(0).compute().call(new PutOneEntryInTxJob());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutTwoEntriesInTx() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            grid(0).compute().call(new PutTwoEntriesInTxJob());

            printCounter();

            assertEquals(GRID_CNT * RETRIES, grid(0).cache(null).get(CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutOneEntryInTxMultiThreaded() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        Collection<Thread> threads = new LinkedList<>();

        try {
            // Initialize.
            grid(0).cache(null).put(CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread("thread-#" + i) {
                    @Override public void run() {
                        retries(grid(gridId), false);
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            printCounter();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutTwoEntryInTxMultiThreaded() throws Exception {
//        resetLog4j(Level.INFO, true, GridCacheTxManager.class.getName());

        startGrids(GRID_CNT);

        Collection<Thread> threads = new LinkedList<>();

        try {
            grid(0).cache(null).put(CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread() {
                    @Override public void run() {
                        retries(grid(gridId), true);
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            printCounter();

            assertEquals(GRID_CNT * RETRIES, grid(0).cache(null).get(CNTR_KEY));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxQueried() throws Exception {
        //resetLog4j(Level.INFO, true, GridCacheTxManager.class.getPackage().getName());

        startGrids(GRID_CNT);

        try {
            IgniteCache<String, Integer> cache = grid(0).cache(null);

            cache.put(RMVD_CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                cache.put(String.valueOf(i), i);

            for (int i = 0; i < RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(String.valueOf(i)));

            Collection<Cache.Entry<String, Integer>> entries =
                cache.query(new SqlQuery<String, Integer>(Integer.class, " _val >= 0")).getAll();

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            grid(0).compute().call(new RemoveInTxJobQueried());

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals(null, grid(ii).cache(null).get(Integer.toString(i)));

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).localPeek(RMVD_CNTR_KEY, CachePeekMode.ONHEAP));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxSimple() throws Exception {
        startGrids(GRID_CNT);

        try {
            IgniteCache<String, Integer> cache = grid(0).cache(null);

            cache.put(RMVD_CNTR_KEY, 0);

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                cache.put(Integer.toString(i), i);

            for (int i = 0; i < RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(Integer.toString(i)));

            Collection<Cache.Entry<String, Integer>> entries =
                cache.query(new SqlQuery<String, Integer>(Integer.class, " _val >= 0")).getAll();

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            grid(0).compute().call(new RemoveInTxJobSimple());

            // Check using cache.
            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals(null, grid(ii).cache(null).get(Integer.toString(i)));

            // Check using query.
            entries = cache.query(new SqlQuery<String, Integer>(Integer.class, " _val >= 0")).getAll();

            assertTrue(entries.isEmpty());

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).localPeek(RMVD_CNTR_KEY, CachePeekMode.ONHEAP));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveInTxQueriedMultiThreaded() throws Exception {
        //resetLog4j(Level.INFO, true, GridCacheTxManager.class.getPackage().getName());

        backups = 1;

        try {
            startGrids(GRID_CNT);

            IgniteCache<String, Integer> cache = grid(0).cache(null);

            // Store counter.
            cache.getAndPut(RMVD_CNTR_KEY, 0);

            // Store values.
            for (int i = 1; i <= GRID_CNT * RETRIES; i++)
                cache.getAndPut(String.valueOf(i), i);

            for (int j = 0; j < GRID_CNT; j++)
                assertEquals(0, grid(j).cache(null).get(RMVD_CNTR_KEY));

            for (int i = 1; i <= RETRIES; i++)
                for (int j = 0; j < GRID_CNT; j++)
                    assertEquals(i, grid(j).cache(null).get(String.valueOf(i)));

            SqlQuery<String, Integer> qry = new SqlQuery<>(Integer.class, "_val >= 0");

            List<Cache.Entry<String, Integer>> entries = new ArrayList<>(cache.query(qry).getAll());

            Collections.sort(entries, new Comparator<Cache.Entry<String, Integer>>() {
                @Override public int compare(Cache.Entry<String, Integer> o1, Cache.Entry<String, Integer> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            info("Queried entries: " + entries);

            int val = 0;

            for (Cache.Entry<String, Integer> e : entries) {
                assertEquals(val, e.getValue().intValue());

                val++;
            }

            assertFalse(entries.isEmpty());

            cntrRmvd.set(0);

            Collection<Thread> threads = new LinkedList<>();

            for (int i = 0; i < GRID_CNT; i++) {
                final int gridId = i;

                threads.add(new Thread() {
                    @Override public void run() {
                        try {
                            removeRetriesQueried(grid(gridId), true);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });
            }

            for (Thread th : threads)
                th.start();

            for (Thread th : threads)
                th.join();

            for (int i = 0; i < GRID_CNT * RETRIES; i++)
                for (int ii = 0; ii < GRID_CNT; ii++)
                    assertEquals("Got invalid value from cache [gridIdx=" + ii + ", key=" + i + ']',
                        null, grid(ii).cache(null).get(Integer.toString(i)));

            assertEquals(-GRID_CNT * RETRIES, grid(0).cache(null).localPeek(RMVD_CNTR_KEY, CachePeekMode.ONHEAP));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private void printCounter() {
        info("***");
        info("*** Peeked counter: " + grid(0).cache(null).localPeek(CNTR_KEY, CachePeekMode.ONHEAP));
        info("*** Got counter: " + grid(0).cache(null).get(CNTR_KEY));
        info("***");
    }

    /**
     * Test job putting data to queue.
     */
    protected class PutTwoEntriesInTxJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            retries(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutTwoEntriesInTxJob.class, this);
        }
    }

    /**
     * Test job putting data to cache.
     */
    protected class PutOneEntryInTxJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            retries(ignite, false);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutOneEntryInTxJob.class, this);
        }
    }

    /**
     * Test job removing data from cache using query.
     */
    protected class RemoveInTxJobQueried implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            removeRetriesQueried(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveInTxJobQueried.class, this);
        }
    }

    /**
     * Test job removing data from cache.
     */
    protected class RemoveInTxJobSimple implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            removeRetriesSimple(ignite, true);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveInTxJobSimple.class, this);
        }
    }
}
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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.junit.Assert.assertArrayEquals;

/**
 * Backups tests.
 */
@SuppressWarnings("unchecked")
public abstract class CacheMvccBackupsAbstractTest extends CacheMvccAbstractTest {

    /** Test timeout. */
    private final long txLongTimeout = getTestTimeout() / 4;

    /**
     * Tests backup consistency.
     *
     * @throws Exception If fails.
     */
    public void testBackupsCoherenceSimple() throws Exception {
        disableScheduledVacuum = true;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, 10)
            .setIndexedTypes(Integer.class, Integer.class);

        final int KEYS_CNT = 5_000;
        assert KEYS_CNT % 2 == 0;

        startGrids(3);

        Ignite node0 = grid(0);
        Ignite node1 = grid(1);
        Ignite node2 = grid(2);

        client = true;

        Ignite client = startGrid();

        awaitPartitionMapExchange();

        IgniteCache clientCache = client.cache(DEFAULT_CACHE_NAME);
        IgniteCache cache0 = node0.cache(DEFAULT_CACHE_NAME);
        IgniteCache cache1 = node1.cache(DEFAULT_CACHE_NAME);
        IgniteCache cache2 = node2.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            for (int i = 0; i < KEYS_CNT / 2; i += 2) {
                SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values ("
                    + i + ',' + i * 2 + "), (" + (i + 1) + ',' + (i + 1) * 2 + ')');

                clientCache.query(qry).getAll();
            }

            tx.commit();
        }

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            for (int i = 0; i < 10; i++) {
                SqlFieldsQuery qry = new SqlFieldsQuery("DELETE from Integer WHERE _key = " + i);

                clientCache.query(qry).getAll();
            }

            for (int i = 10; i < KEYS_CNT + 1; i++) {
                SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val=" + i * 10 + " WHERE _key = " + i);

                clientCache.query(qry).getAll();
            }

            tx.commit();
        }

        Map<KeyCacheObject, List<CacheDataRow>> vers0 = allVersions(cache0);

        List res0 = getAll(cache0, "Integer");

        stopGrid(0);

        awaitPartitionMapExchange();

        Map<KeyCacheObject, List<CacheDataRow>> vers1 = allVersions(cache1);

        assertVersionsEquals(vers0, vers1);

        List res1 = getAll(cache1, "Integer");

        assertEqualsCollections(res0, res1);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            for (int i = 10; i < 20; i++) {
                SqlFieldsQuery qry = new SqlFieldsQuery("DELETE from Integer WHERE _key = " + i);

                clientCache.query(qry).getAll();
            }

            for (int i = 20; i < KEYS_CNT + 1; i++) {
                SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val=" + i * 100 + " WHERE _key = " + i);

                clientCache.query(qry).getAll();
            }

            tx.commit();
        }

        vers1 = allVersions(cache1);

        res1 = getAll(cache2, "Integer");

        stopGrid(1);

        awaitPartitionMapExchange();

        Map<KeyCacheObject, List<CacheDataRow>> vers2 = allVersions(cache2);

        assertVersionsEquals(vers1, vers2);

        List res2 = getAll(cache2, "Integer");

        assertEqualsCollections(res1, res2);
    }

    /**
     * Checks cache backups consistency with large queries.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithLargeOperations() throws Exception {
        disableScheduledVacuum = true;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 1, 10)
            .setIndexedTypes(Integer.class, Integer.class);

        final int KEYS_CNT = 5_000;
        assert KEYS_CNT % 2 == 0;

        startGrids(2);

        Ignite node1 = grid(0);
        Ignite node2 = grid(1);

        client = true;

        Ignite client = startGrid();

        awaitPartitionMapExchange();

        IgniteCache clientCache = client.cache(DEFAULT_CACHE_NAME);
        IgniteCache cache1 = node1.cache(DEFAULT_CACHE_NAME);
        IgniteCache cache2 = node2.cache(DEFAULT_CACHE_NAME);

        StringBuilder insert = new StringBuilder("INSERT INTO Integer (_key, _val) values ");

        boolean first = true;

        for (int key = 0; key < KEYS_CNT; key++) {
            if (!first)
                insert.append(',');
            else
                first = false;

            insert.append('(').append(key).append(',').append(key * 10).append(')');
        }

        String qryStr = insert.toString();

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            SqlFieldsQuery qry = new SqlFieldsQuery(qryStr);

            clientCache.query(qry).getAll();

            tx.commit();
        }

        qryStr = "SELECT * FROM Integer WHERE _key >= " + KEYS_CNT / 2 + " FOR UPDATE";

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            SqlFieldsQuery qry = new SqlFieldsQuery(qryStr);

            clientCache.query(qry).getAll();

            tx.commit();
        }


        qryStr = "DELETE FROM Integer WHERE _key >= " + KEYS_CNT / 2;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            SqlFieldsQuery qry = new SqlFieldsQuery(qryStr);

            clientCache.query(qry).getAll();

            tx.commit();
        }

        Map<KeyCacheObject, List<CacheDataRow>> cache1Vers = allVersions(cache1);

        List res1 = getAll(cache1, "Integer");

        stopGrid(0);

        awaitPartitionMapExchange();

        Map<KeyCacheObject, List<CacheDataRow>> cache2Vers = allVersions(cache2);

        assertVersionsEquals(cache1Vers, cache2Vers);

        List res2 = getAll(cache2, "Integer");

        assertEqualsCollections(res1, res2);
    }

    /**
     * Checks cache backups consistency with in-flight batches overflow.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithInFlightBatchesOverflow() throws Exception {
        testSpi = true;

        disableScheduledVacuum = true;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 1, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        final int KEYS_CNT = 30_000;
        assert KEYS_CNT % 2 == 0;

        startGrids(2);

        Ignite node1 = grid(0);
        Ignite node2 = grid(1);

        client = true;

        Ignite client = startGrid();

        awaitPartitionMapExchange();

        IgniteCache<?,?> clientCache = client.cache(DEFAULT_CACHE_NAME);
        IgniteCache<?,?> cache1 = node1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<?,?> cache2 = node2.cache(DEFAULT_CACHE_NAME);

        AtomicInteger keyGen = new AtomicInteger();
        Affinity affinity = affinity(clientCache);

        ClusterNode cNode1 = ((IgniteEx)node1).localNode();
        ClusterNode cNode2 = ((IgniteEx)node2).localNode();

        StringBuilder insert = new StringBuilder("INSERT INTO Integer (_key, _val) values ");

        for (int i = 0; i < KEYS_CNT; i++) {
            if (i > 0)
                insert.append(',');

            // To make big batches in near results future.
            Integer key = i < KEYS_CNT / 2 ? keyForNode(affinity, keyGen, cNode1) : keyForNode(affinity, keyGen, cNode2);

            assert key != null;

            insert.append('(').append(key).append(',').append(key * 10).append(')');
        }

        String qryStr = insert.toString();

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            SqlFieldsQuery qry = new SqlFieldsQuery(qryStr);

            clientCache.query(qry).getAll();

            tx.commit();
        }

        // Add a delay to simulate batches overflow.
        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(node1);
        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(node2);

        spi1.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            @Override public void apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxQueryEnlistResponse)
                    doSleep(100);
            }
        });

        spi2.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            @Override public void apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxQueryEnlistResponse)
                    doSleep(100);
            }
        });

        qryStr = "DELETE FROM Integer WHERE _key >= " + 10;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(txLongTimeout);

            SqlFieldsQuery qry = new SqlFieldsQuery(qryStr);

            clientCache.query(qry).getAll();

            tx.commit();
        }

        Map<KeyCacheObject, List<CacheDataRow>> cache1Vers = allVersions(cache1);

        List res1 = getAll(cache1, "Integer");

        stopGrid(0);

        awaitPartitionMapExchange();

        Map<KeyCacheObject, List<CacheDataRow>> cache2Vers = allVersions(cache2);

        assertVersionsEquals(cache1Vers, cache2Vers);

        List res2 = getAll(cache2, "Integer");

        assertEqualsCollections(res1, res2);
    }

    /**
     * Tests concurrent updates backups coherence.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithConcurrentUpdates2ServersNoClients() throws Exception {
        checkBackupsCoherenceWithConcurrentUpdates(2, 0);
    }

    /**
     * Tests concurrent updates backups coherence.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithConcurrentUpdates4ServersNoClients() throws Exception {
        checkBackupsCoherenceWithConcurrentUpdates(4, 0);
    }

    /**
     * Tests concurrent updates backups coherence.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithConcurrentUpdates3Servers1Client() throws Exception {
        checkBackupsCoherenceWithConcurrentUpdates(3, 1);
    }

    /**
     * Tests concurrent updates backups coherence.
     *
     * @throws Exception If failed.
     */
    public void testBackupsCoherenceWithConcurrentUpdates5Servers2Clients() throws Exception {
        checkBackupsCoherenceWithConcurrentUpdates(5, 2);
    }

    /**
     * Tests concurrent updates backups coherence.
     *
     * @throws Exception If failed.
     */
    private void checkBackupsCoherenceWithConcurrentUpdates(int srvs, int clients) throws Exception {
        assert srvs > 1;

        disableScheduledVacuum = true;

        accountsTxReadAll(srvs, clients, srvs - 1, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, 5_000, null);

        for (int i = 0; i < srvs - 1; i++) {
            Ignite node1 = grid(i);

            IgniteCache cache1 = node1.cache(DEFAULT_CACHE_NAME);

            Map<KeyCacheObject, List<CacheDataRow>> vers1 = allVersions(cache1);

            List res1 = getAll(cache1, "MvccTestAccount");

            stopGrid(i);

            awaitPartitionMapExchange();

            Ignite node2 = grid(i + 1);

            IgniteCache cache2 = node2.cache(DEFAULT_CACHE_NAME);

            Map<KeyCacheObject, List<CacheDataRow>> vers2 = allVersions(cache2);

            assertVersionsEquals(vers1, vers2);

            List res2 = getAll(cache2, "MvccTestAccount");

            assertEqualsCollections(res1, res2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoForceKeyRequestDelayedRebalanceNoVacuum() throws Exception {
        disableScheduledVacuum = true;

        doTestRebalanceNodeAdd(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoForceKeyRequestDelayedRebalance() throws Exception {
        doTestRebalanceNodeAdd(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoForceKeyRequestNoVacuum() throws Exception {
        disableScheduledVacuum = true;

        doTestRebalanceNodeAdd(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoForceKeyRequest() throws Exception {
        doTestRebalanceNodeAdd(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestRebalanceNodeAdd(boolean delayRebalance) throws Exception {
        testSpi = true;

        final Ignite node1 = startGrid(0);

        final IgniteCache<Object, Object> cache = node1.createCache(
            cacheConfiguration(cacheMode(), FULL_SYNC, 1, 16)
                .setIndexedTypes(Integer.class, Integer.class));

        try (Transaction tx = node1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values " +
                "(1,1),(2,2),(3,3),(4,4),(5,5)");

            cache.query(qry).getAll();

            tx.commit();
        }

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(node1);

        // Check for a force key request.
        spi.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            @Override public void apply(ClusterNode node, Message msg) {
                if (delayRebalance && msg instanceof GridDhtPartitionSupplyMessage)
                    doSleep(500);

                if (msg instanceof GridDhtForceKeysResponse)
                    fail("Force key request");
            }
        });

        final Ignite node2 = startGrid(1);

        TestRecordingCommunicationSpi.spi(node2).closure(
            new IgniteBiInClosure<ClusterNode, Message>() {
                @Override public void apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtForceKeysRequest)
                        fail("Force key request");
                }
            }
        );

        IgniteCache<Object, Object> cache2 = node2.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key IN " +
                "(1,2,3,4,5)");

            cache2.query(qry).getAll();

            tx.commit();
        }

        awaitPartitionMapExchange();

        doSleep(2000);

        stopGrid(1);

        try (Transaction tx = node1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values " +
                "(1,1),(2,2),(3,3),(4,4),(5,5)");

            cache.query(qry).getAll();

            tx.commit();
        }

        doSleep(1000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceNodeLeaveClient() throws Exception {
        doTestRebalanceNodeLeave(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceNodeLeaveServer() throws Exception {
        doTestRebalanceNodeLeave(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void doTestRebalanceNodeLeave(boolean startClient) throws Exception {
        testSpi = true;
        disableScheduledVacuum = true;

        startGridsMultiThreaded(4);

        client = true;

        final Ignite node = startClient ? startGrid(4) : grid(0);

        final IgniteCache<Object, Object> cache = node.createCache(
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, 16)
                .setIndexedTypes(Integer.class, Integer.class));

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 4; i++)
            keys.addAll(primaryKeys(grid(i).cache(DEFAULT_CACHE_NAME), 2));

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            StringBuilder sb = new StringBuilder("INSERT INTO Integer (_key, _val) values ");

            for (int i = 0; i < keys.size(); i++) {
                if (i > 0)
                    sb.append(", ");

                sb.append("(" + keys.get(i) + ", " + keys.get(i) + ")");
            }

            SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());

            cache.query(qry).getAll();

            tx.commit();
        }

        stopGrid(3);

        awaitPartitionMapExchange();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = 10*_key");

            cache.query(qry).getAll();

            tx.commit();
        }

        awaitPartitionMapExchange();

        for (Integer key : keys) {
            List<CacheDataRow> vers = null;

            for (int i = 0; i < 3; i++) {
                ClusterNode n = grid(i).cluster().localNode();

                if (node.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(n, key)) {

                    List<CacheDataRow> vers0 = allKeyVersions(grid(i).cache(DEFAULT_CACHE_NAME), key);

                    if (vers != null)
                        assertKeyVersionsEquals(vers, vers0);

                    vers = vers0;
                }
            }
        }
    }

    /**
     * Retrieves all versions of all keys from cache.
     *
     * @param cache Cache.
     * @return {@link Map} of keys to its versions.
     * @throws IgniteCheckedException If failed.
     */
    private Map<KeyCacheObject, List<CacheDataRow>> allVersions(IgniteCache cache) throws IgniteCheckedException {
        IgniteCacheProxy cache0 = (IgniteCacheProxy)cache;
        GridCacheContext cctx = cache0.context();

        assert cctx.mvccEnabled();

        Map<KeyCacheObject, List<CacheDataRow>> vers = new HashMap<>();

        for (Object e : cache) {
            IgniteBiTuple entry = (IgniteBiTuple)e;

            KeyCacheObject key = cctx.toCacheKeyObject(entry.getKey());

            GridCursor<CacheDataRow> cur = cctx.offheap().mvccAllVersionsCursor(cctx, key, null);

            List<CacheDataRow> rows = new ArrayList<>();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                rows.add(row);
            }

            vers.put(key, rows);
        }

        return vers;
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return Collection of versioned rows.
     * @throws IgniteCheckedException if failed.
     */
    private List<CacheDataRow> allKeyVersions(IgniteCache cache, Object key) throws IgniteCheckedException {
        IgniteCacheProxy cache0 = (IgniteCacheProxy)cache;
        GridCacheContext cctx = cache0.context();

        KeyCacheObject key0 = cctx.toCacheKeyObject(key);

        GridCursor<CacheDataRow> cur = cctx.offheap().mvccAllVersionsCursor(cctx, key0, null);

        List<CacheDataRow> rows = new ArrayList<>();

        while (cur.next()) {
            CacheDataRow row = cur.get();

            rows.add(row);
        }

        return rows;
    }

    /**
     * Checks stored versions equality.
     *
     * @param left Keys versions to compare.
     * @param right Keys versions to compare.
     * @throws IgniteCheckedException If failed.
     */
    private void assertVersionsEquals(Map<KeyCacheObject, List<CacheDataRow>> left,
        Map<KeyCacheObject, List<CacheDataRow>> right) throws IgniteCheckedException {
        assertNotNull(left);
        assertNotNull(right);

        assertTrue(!left.isEmpty());
        assertTrue(!right.isEmpty());

        assertEqualsCollections(left.keySet(), right.keySet());

        for (KeyCacheObject key : right.keySet()) {
            List<CacheDataRow> leftRows = left.get(key);
            List<CacheDataRow> rightRows = right.get(key);

            assertKeyVersionsEquals(leftRows, rightRows);
        }
    }

    /**
     *
     * @param leftRows Left rows.
     * @param rightRows Right rows.
     * @throws IgniteCheckedException If failed.
     */
    private void assertKeyVersionsEquals(List<CacheDataRow> leftRows, List<CacheDataRow> rightRows)
        throws IgniteCheckedException {

        assertNotNull(leftRows);
        assertNotNull(rightRows);

        assertEquals("leftRows=" + leftRows + ", rightRows=" + rightRows, leftRows.size(), rightRows.size());

        for (int i = 0; i < leftRows.size(); i++) {
            CacheDataRow leftRow = leftRows.get(i);
            CacheDataRow rightRow = rightRows.get(i);

            assertNotNull(leftRow);
            assertNotNull(rightRow);

            assertTrue(leftRow instanceof MvccDataRow);
            assertTrue(rightRow instanceof MvccDataRow);

            leftRow.key().valueBytes(null);

            assertEquals(leftRow.expireTime(), rightRow.expireTime());
            assertEquals(leftRow.partition(), rightRow.partition());
            assertArrayEquals(leftRow.value().valueBytes(null), rightRow.value().valueBytes(null));
            assertEquals(leftRow.version(), rightRow.version());
            assertEquals(leftRow.cacheId(), rightRow.cacheId());
            assertEquals(leftRow.hash(), rightRow.hash());
            assertEquals(leftRow.key(), rightRow.key());
            assertTrue(MvccUtils.compare(leftRow, rightRow.mvccVersion()) == 0);
            assertTrue(MvccUtils.compareNewVersion(leftRow, rightRow.newMvccVersion()) == 0);
            assertEquals(leftRow.newMvccCoordinatorVersion(), rightRow.newMvccCoordinatorVersion());
            assertEquals(leftRow.newMvccCounter(), rightRow.newMvccCounter());
            assertEquals(leftRow.newMvccOperationCounter(), rightRow.newMvccOperationCounter());
        }
    }

    /**
     * Retrieves all table rows from local node.
     * @param cache Cache.
     * @param tblName Table name.
     * @return All table rows.
     */
    private List getAll(IgniteCache cache, String tblName) {
        List res = cache.query(new SqlFieldsQuery("SELECT * FROM " + tblName)).getAll();

        Collections.sort(res, new Comparator<Object>() {
            @Override public int compare(Object o1, Object o2) {
                List<?> l1 = (List<?>)o1;
                List<?> l2 = (List<?>)o2;

                int res =  ((Comparable)l1.get(0)).compareTo((Comparable)l2.get(0));

                if (res == 0 && l1.size() > 1)
                    return ((Comparable)l1.get(1)).compareTo((Comparable)l2.get(1));
                else
                    return res;

            }
        });

        return res;
    }
}

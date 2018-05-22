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

package org.apache.ignite.internal.processors.cache.index;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi;

/**
 * Concurrency tests for dynamic index create/drop.
 */
@SuppressWarnings("unchecked")
public abstract class DynamicColumnsAbstractConcurrentSelfTest extends DynamicColumnsAbstractTest {
    /** Test duration. */
    private static final long TEST_DUR = 10_000L;

    /** Large cache size. */
    private static final int LARGE_CACHE_SIZE = 100_000;

    /** Table name. */
    private static final String TBL_NAME = "PERSON";

    /** Cache name. */
    private static final String CACHE_NAME = QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, TBL_NAME);

    /** Attribute to filter node out of cache data nodes. */
    private static final String ATTR_FILTERED = "FILTERED";

    /** SQL statement to create test table accompanied by template specification. */
    private final String createSql;

    /** SQL statement to create test table with additional columns. */
    private final String createSql4Cols;

    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T3<CountDownLatch, AtomicBoolean, CountDownLatch>> BLOCKS =
        new ConcurrentHashMap<>();

    /** Cache mode. */
    private final CacheMode cacheMode;

    /** Atomicity mode. */
    private final CacheAtomicityMode atomicityMode;

    /**
     * Constructor.
     *
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    DynamicColumnsAbstractConcurrentSelfTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;

        final String template = " WITH \"template=TPL\"";

        createSql =  CREATE_SQL + template;
        createSql4Cols = CREATE_SQL_4_COLS + template;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridQueryProcessor.idxCls = BlockingIndexing.class;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected void afterTest() throws Exception {
        GridQueryProcessor.idxCls = null;

        for (T3<CountDownLatch, AtomicBoolean, CountDownLatch> block : BLOCKS.values())
            block.get1().countDown();

        BLOCKS.clear();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        TestTcpDiscoverySpi spi = new TestTcpDiscoverySpi();
        spi.setIpFinder(LOCAL_IP_FINDER);

        return super.commonConfiguration(idx)
            .setDiscoverySpi(spi);
    }

    /**
     * Make sure that coordinator migrates correctly between nodes.
     *
     * @throws Exception If failed.
     */
    public void testAddColumnCoordinatorChange() throws Exception {
        checkCoordinatorChange(true);
    }

    /**
     * Make sure that coordinator migrates correctly between nodes.
     *
     * @throws Exception If failed.
     */
    public void testDropColumnCoordinatorChange() throws Exception {
        checkCoordinatorChange(false);
    }

    /**
     * Make sure that coordinator migrates correctly between nodes.
     *
     * @param addOrRemove Pass {@code true} to check add column. Otherwise, drop column is checked.
     * @throws Exception If failed.
     */
    public void checkCoordinatorChange(boolean addOrRemove) throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(2);

        // Start servers.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1), null);
        IgniteEx srv2 = ignitionStart(serverConfiguration(2), null);
        IgniteEx srv3 = ignitionStart(serverConfiguration(3, true), finishLatch);

        UUID srv1Id = srv1.cluster().localNode().id();
        UUID srv2Id = srv2.cluster().localNode().id();

        // Start client which will execute operations.
        IgniteEx cli = ignitionStart(clientConfiguration(4), finishLatch);

        createSqlCache(cli);

        run(cli, addOrRemove ? createSql : createSql4Cols);

        // Test migration between normal servers.
        CountDownLatch idxLatch = blockIndexing(srv1Id);

        IgniteInternalFuture<?> colFut1 = addOrRemove ?
            addCols(cli, QueryUtils.DFLT_SCHEMA, c("age", Integer.class.getName())) :
            dropCols(cli, QueryUtils.DFLT_SCHEMA, "AGE");

        U.await(idxLatch);

        //srv1.close();
        Ignition.stop(srv1.name(), true);

        unblockIndexing(srv1Id);

        colFut1.get();

        // Port number is for srv2.
        checkTableState(srv2, QueryUtils.DFLT_SCHEMA, TBL_NAME,
            addOrRemove ? c("age", Integer.class.getName()) : c("CITY", String.class.getName()));

        // Test migration from normal server to non-affinity server.
        idxLatch = blockIndexing(srv2Id);

        IgniteInternalFuture<?> colFut2 = addOrRemove ?
            addCols(cli, QueryUtils.DFLT_SCHEMA, c("city", String.class.getName())) :
            dropCols(cli, QueryUtils.DFLT_SCHEMA, "CITY");

        idxLatch.countDown();

        //srv2.close();
        Ignition.stop(srv2.name(), true);

        U.await(idxLatch);

        colFut2.get();

        // Let's actually create cache on non affinity server.
        srv3.cache(QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, "PERSON"));

        // Port number is for srv3.
        checkTableState(srv3, QueryUtils.DFLT_SCHEMA, TBL_NAME,
            addOrRemove ? c("city", String.class.getName()) : c("NAME", String.class.getName()));
    }

    /**
     * Test operations join.
     *
     * @throws Exception If failed.
     */
    public void testOperationChaining() throws Exception {
        // 7 nodes * 2 columns = 14 latch countdowns.
        CountDownLatch finishLatch = new CountDownLatch(14);

        IgniteEx srv1 = ignitionStart(serverConfiguration(1), finishLatch);

        ignitionStart(serverConfiguration(2), finishLatch);
        ignitionStart(serverConfiguration(3, true), finishLatch);
        ignitionStart(clientConfiguration(4), finishLatch);

        createSqlCache(srv1);

        run(srv1, createSql);

        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryField c0 = c("ID", Integer.class.getName());
        QueryField c1 = c("NAME", String.class.getName());
        QueryField c2 = c("age", Integer.class.getName());
        QueryField c3 = c("city", String.class.getName());

        IgniteInternalFuture<?> colFut1 = addCols(srv1, QueryUtils.DFLT_SCHEMA, c2);

        IgniteInternalFuture<?> colFut2 = dropCols(srv1, QueryUtils.DFLT_SCHEMA, c1.name());

        IgniteInternalFuture<?> colFut3 = addCols(srv1, QueryUtils.DFLT_SCHEMA, c3);

        U.await(idxLatch);

        // Start even more nodes of different flavors
        ignitionStart(serverConfiguration(5), finishLatch);
        ignitionStart(serverConfiguration(6, true), finishLatch);
        ignitionStart(clientConfiguration(7), finishLatch);

        assert !colFut1.isDone();
        assert !colFut2.isDone();
        assert !colFut3.isDone();

        unblockIndexing(srv1);

        colFut1.get();
        colFut2.get();
        colFut3.get();

        U.await(finishLatch);

        checkTableState(srv1, QueryUtils.DFLT_SCHEMA, TBL_NAME, c0, c2, c3);
    }

    /**
     * Test node join on pending add column operation.
     *
     * @throws Exception If failed.
     */
    public void testNodeJoinOnPendingAddOperation() throws Exception {
        checkNodeJoinOnPendingOperation(true);
    }

    /**
     * Test node join on pending drop column operation.
     *
     * @throws Exception If failed.
     */
    public void testNodeJoinOnPendingDropOperation() throws Exception {
        checkNodeJoinOnPendingOperation(false);
    }

    /**
     * Check node join on pending operation.
     *
     * @param addOrRemove Pass {@code true} to check add column. Otherwise, drop column is checked.
     * @throws Exception If failed.
     */
    private void checkNodeJoinOnPendingOperation(boolean addOrRemove) throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(3);

        IgniteEx srv1 = ignitionStart(serverConfiguration(1), finishLatch);

        createSqlCache(srv1);

        run(srv1, addOrRemove ? createSql : createSql4Cols);

        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryField c = c("AGE", Integer.class.getName());

        IgniteInternalFuture<?> idxFut = addOrRemove ? addCols(srv1, QueryUtils.DFLT_SCHEMA, c) :
            dropCols(srv1, QueryUtils.DFLT_SCHEMA, "CITY");

        U.await(idxLatch);

        ignitionStart(serverConfiguration(2), finishLatch);
        ignitionStart(serverConfiguration(3, true), finishLatch);

        assertFalse(idxFut.isDone());

        unblockIndexing(srv1);

        idxFut.get();

        U.await(finishLatch);

        checkTableState(srv1, QueryUtils.DFLT_SCHEMA, TBL_NAME, c);
    }

    /**
     * PUT/REMOVE data from cache and add/drop column concurrently.
     *
     * @throws Exception If failed,
     */
    public void testConcurrentPutRemove() throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(4);

        // Start several nodes.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1), finishLatch);
        ignitionStart(serverConfiguration(2), finishLatch);
        ignitionStart(serverConfiguration(3), finishLatch);
        ignitionStart(serverConfiguration(4), finishLatch);

        awaitPartitionMapExchange();

        createSqlCache(srv1);

        run(srv1, createSql4Cols);

        // Start data change operations from several threads.
        final AtomicBoolean stopped = new AtomicBoolean();

        IgniteInternalFuture updateFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    int key = ThreadLocalRandom.current().nextInt(0, LARGE_CACHE_SIZE);
                    int val = ThreadLocalRandom.current().nextInt();

                    IgniteCache<Object, BinaryObject> cache = node.cache(CACHE_NAME);

                    if (ThreadLocalRandom.current().nextBoolean())
                        cache.put(key(key), val(node, val));
                    else
                        cache.remove(key(key));
                }

                return null;
            }
        }, 4);

        // Let some to arrive.
        Thread.sleep(500L);

        addCols(srv1, QueryUtils.DFLT_SCHEMA, c("v", Integer.class.getName())).get();

        dropCols(srv1, QueryUtils.DFLT_SCHEMA, "CITY").get();

        // Stop updates once index is ready.
        stopped.set(true);

        updateFut.get();

        finishLatch.await();

        // Make sure new column is there.
        checkTableState(srv1, QueryUtils.DFLT_SCHEMA, TBL_NAME, c("AGE", Integer.class.getName()),
            c("v", Integer.class.getName()));

        run(srv1, "update person set \"v\" = case when mod(id, 2) <> 0 then substring(name, 7, length(name) - 6) " +
            "else null end");

        // Get expected values.
        Set<Integer> expKeys = new HashSet<>();

        IgniteCache<Object, BinaryObject> cache = srv1.cache(CACHE_NAME).withKeepBinary();

        for (int i = 0; i < LARGE_CACHE_SIZE; i++) {
            Object key = key(i);

            BinaryObject val = cache.get(key);

            if (val != null) {
                int id = (Integer)key;

                assertEquals(i, id);

                if (id % 2 != 0)
                    expKeys.add(i);
            }
        }

        String valTypeName = (srv1).context().query().types(CACHE_NAME).iterator().next().valueTypeName();

        // Validate query result.
        for (Ignite node : Ignition.allGrids()) {
            IgniteCache<Object, BinaryObject> nodeCache = node.cache(CACHE_NAME).withKeepBinary();

            SqlQuery qry = new SqlQuery(valTypeName, "from " + TBL_NAME + " where mod(id, 2) <> 0");

            List<Cache.Entry<Object, BinaryObject>> res = nodeCache.query(qry).getAll();

            assertEquals("Cache size mismatch [exp=" + expKeys.size() + ", actual=" + res.size() + ']',
                expKeys.size(), res.size());

            for (Cache.Entry<Object, BinaryObject> entry : res) {
                int key = (Integer)entry.getKey();
                int v = entry.getValue().field("v");

                String name = entry.getValue().field("NAME");

                assertTrue("Expected key is not in result set: " + key, expKeys.contains(key));

                assertEquals(Integer.parseInt(name.substring(6)), v);
            }

        }
    }

    /**
     * @param node Node.
     * @param val Number to form string field value from.
     * @return PERSON cache value.
     */
    private BinaryObject val(Ignite node, int val) {
        String valTypeName = ((IgniteEx)node).context().query().types(CACHE_NAME).iterator().next().valueTypeName();

        return node.binary().builder(valTypeName).setField("name", "person" + val).build();
    }

    /**
     * @param id Key.
     * @return PERSON cache key (int or {@link BinaryObject}).
     */
    private Object key(int id) {
        return id;
    }

    /**
     * Test index consistency on re-balance.
     *
     * @throws Exception If failed.
     */
    public void testAddConcurrentRebalance() throws Exception {
        checkConcurrentRebalance(true);
    }

    /**
     * Test index consistency on re-balance.
     *
     * @throws Exception If failed.
     */
    public void testDropConcurrentRebalance() throws Exception {
        checkConcurrentRebalance(false);
    }

    /**
     * Check index consistency on re-balance.
     *
     * @param addOrRemove Pass {@code true} to check add column. Otherwise, drop column is checked.
     * @throws Exception If failed.
     */
    public void checkConcurrentRebalance(boolean addOrRemove) throws Exception {
        // Start cache and populate it with data.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));
        Ignite srv2 = ignitionStart(serverConfiguration(2));

        createSqlCache(srv1);

        run(srv1, createSql);

        awaitPartitionMapExchange();

        put(srv1, 0, LARGE_CACHE_SIZE);

        // Start index operation in blocked state.
        CountDownLatch idxLatch1 = blockIndexing(srv1);
        CountDownLatch idxLatch2 = blockIndexing(srv2);

        QueryField c = c("salary", Double.class.getName());

        final IgniteInternalFuture<?> idxFut = addOrRemove ?
            addCols(srv1, QueryUtils.DFLT_SCHEMA, c) : dropCols(srv1, QueryUtils.DFLT_SCHEMA, "NAME");

        U.await(idxLatch1);
        U.await(idxLatch2);

        // Start two more nodes and unblock index operation in the middle.
        ignitionStart(serverConfiguration(3));

        unblockIndexing(srv1);
        unblockIndexing(srv2);

        ignitionStart(serverConfiguration(4));

        awaitPartitionMapExchange();

        // Validate index state.
        idxFut.get();

        checkTableState(srv1, QueryUtils.DFLT_SCHEMA, TBL_NAME, addOrRemove ? c : c("ID", Integer.class.getName()));
    }

    /**
     * Put to cache keys and values for range from startIdx to endIdx.
     * @param node Node.
     * @param startIdx Starting index.
     * @param endIdx Ending index.
     */
    private void put(Ignite node, int startIdx, int endIdx) {
        for (int i = startIdx; i < endIdx; i++)
            node.cache(CACHE_NAME).put(key(i), val(node, i));
    }

    /**
     * Check what happens in case cache is destroyed before operation is started.
     *
     * @throws Exception If failed.
     */
    public void testAddConcurrentCacheDestroy() throws Exception {
        checkConcurrentCacheDestroy(true);
    }

    /**
     * Check what happens in case cache is destroyed before operation is started.
     *
     * @throws Exception If failed.
     */
    public void testDropConcurrentCacheDestroy() throws Exception {
        checkConcurrentCacheDestroy(false);
    }

    /**
     * Check what happens in case cache is destroyed before operation is started.
     *
     * @param addOrRemove Pass {@code true} to check add column. Otherwise, drop column is checked.
     * @throws Exception If failed.
     */
    private void checkConcurrentCacheDestroy(boolean addOrRemove) throws Exception {
        // Start complex topology.
        Ignite srv1 = ignitionStart(serverConfiguration(1));

        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        waitForDiscovery(srv1, grid(2), grid(3), cli);

        // Start cache and populate it with data.
        createSqlCache(cli);

        run(cli, createSql);

        put(cli, 0, LARGE_CACHE_SIZE);

        // Start index operation and block it on coordinator.
        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryField c = c("city", String.class.getName());

        final IgniteInternalFuture<?> idxFut = addOrRemove ?
            addCols(srv1, QueryUtils.DFLT_SCHEMA, c) : dropCols(srv1, QueryUtils.DFLT_SCHEMA, "NAME");

        idxLatch.await();

        // Destroy cache (drop table).
        run(cli, DROP_SQL);

        // Unblock indexing and see what happens.
        unblockIndexing(srv1);

        try {
            idxFut.get();

            fail("Exception has not been thrown.");
        }
        catch (SchemaOperationException e) {
            // No-op.
        }
    }

    /**
     * Make sure that contended operations on the same table from different nodes do not hang when we issue both
     * ADD/DROP COLUMN and SELECT statements.
     *
     * @throws Exception If failed.
     */
    public void testQueryConsistencyMultithreaded() throws Exception {
        // Start complex topology.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        run(cli, createSql);

        put(cli, 0, 5000);

        final AtomicBoolean stopped = new AtomicBoolean();

        final AtomicInteger dynColCnt = new AtomicInteger();

        final GridConcurrentHashSet<Integer> fields = new GridConcurrentHashSet<>();

        IgniteInternalFuture fut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    int fieldNum = ThreadLocalRandom.current().nextInt(0, dynColCnt.get() + 1);

                    boolean removed = fields.remove(fieldNum);

                    if (removed)
                        fut = dropCols(node, QueryUtils.DFLT_SCHEMA, "newCol" + fieldNum);
                    else {
                        fieldNum = dynColCnt.getAndIncrement();

                        fut = addCols(node, QueryUtils.DFLT_SCHEMA, c("newCol" + fieldNum,
                            Integer.class.getName()));
                    }
                    try {
                        fut.get();

                        if (!removed)
                            fields.add(fieldNum);
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 1);

        IgniteInternalFuture qryFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteCache<BinaryObject, BinaryObject> cache = node.cache(CACHE_NAME).withKeepBinary();

                    String valTypeName = ((IgniteEx)node).context().query().types(CACHE_NAME)
                        .iterator().next().valueTypeName();

                    List<Cache.Entry<BinaryObject, BinaryObject>> res = cache.query(
                        new SqlQuery<BinaryObject, BinaryObject>(valTypeName, "from " + TBL_NAME)).getAll();

                    assertEquals(5000, res.size());
                }

                return null;
            }
        }, 8);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        fut.get();
        qryFut.get();
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected.
     *
     * @throws Exception If failed.
     */
    public void testClientReconnect() throws Exception {
        checkClientReconnect(false, true);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected, even with cache recreation.
     *
     * @throws Exception If failed.
     */
    public void testClientReconnectWithCacheRestart() throws Exception {
        checkClientReconnect(true, true);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected.
     *
     * @throws Exception If failed.
     */
    public void testClientReconnectWithNonDynamicCache() throws Exception {
        checkClientReconnect(false, false);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected, even with cache recreation.
     *
     * @throws Exception If failed.
     */
    public void testClientReconnectWithNonDynamicCacheRestart() throws Exception {
        checkClientReconnect(true, false);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected, optionally with cache restart
     * in the interim.
     *
     * @param restartCache Whether cache needs to be recreated during client's absence.
     * @param dynamicCache Whether recreate, if needed, should be done on dynamic or static cache.
     * @throws Exception If failed.
     */
    private void checkClientReconnect(final boolean restartCache, boolean dynamicCache) throws Exception {
        // Start complex topology.
        final IgniteEx srv = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        final Ignite cli = ignitionStart(clientConfiguration(4));

        if (dynamicCache) {
            createSqlCache(cli);

            run(cli, createSql);
        }

        final String schemaName = dynamicCache ? QueryUtils.DFLT_SCHEMA : "idx";

        final QueryField[] cols =
            new QueryField[] { c("age", Integer.class.getName()), c("city", String.class.getName()) };

        // Check index create.
        reconnectClientNode(srv, cli, restartCache, dynamicCache, new RunnableX() {
            @Override public void run() throws Exception {
                addCols(srv, schemaName, cols).get();

                dropCols(srv, schemaName, "NAME").get();
            }
        });

        checkTableState(srv, schemaName, TBL_NAME, cols);
    }

    /**
     * Reconnect the client and run specified actions while it's out.
     *
     * @param srvNode Server node.
     * @param cliNode Client node.
     * @param restart Whether cache has to be recreated prior to executing required actions.
     * @param dynamicCache Whether recreate, if needed, should be done on dynamic or static cache.
     * @param clo Closure to run
     * @throws Exception If failed.
     */
    private void reconnectClientNode(final Ignite srvNode, final Ignite cliNode, final boolean restart,
        final boolean dynamicCache, final RunnableX clo) throws Exception {
        IgniteClientReconnectAbstractTest.reconnectClientNode(log, cliNode, srvNode, new Runnable() {
            @Override public void run() {
                if (restart) {
                    if (dynamicCache) {
                        DynamicColumnsAbstractConcurrentSelfTest.this.run(srvNode, DROP_SQL);

                        DynamicColumnsAbstractConcurrentSelfTest.this.run(srvNode, createSql);
                    }
                    else {
                        srvNode.destroyCache("idx");

                        CacheConfiguration ccfg;

                        try {
                            ccfg = clientConfiguration(0).getCacheConfiguration()[0];
                        }
                        catch (Exception e) {
                            throw new AssertionError(e);
                        }

                        srvNode.createCache(ccfg);
                    }
                }

                try {
                    clo.run();
                }
                catch (Exception e) {
                    throw new IgniteException("Test reconnect runnable failed.", e);
                }
            }
        });

        if (restart)
            cliNode.cache(CACHE_NAME);
    }

    /**
     * Test concurrent node start/stop along with add/drop column operations. Nothing should hang.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("StringConcatenationInLoop")
    public void testConcurrentOperationsAndNodeStartStopMultithreaded() throws Exception {
        // Start several stable nodes.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        final IgniteEx cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        run(cli, createSql);

        final AtomicBoolean stopped = new AtomicBoolean();

        // Start node start/stop worker.
        final AtomicInteger nodeIdx = new AtomicInteger(4);

        final AtomicInteger dynColCnt = new AtomicInteger();

        IgniteInternalFuture startStopFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                int lastIdx = 0;

                while (!stopped.get()) {
                    if (exists) {
                        stopGrid(lastIdx);

                        exists = false;
                    }
                    else {
                        lastIdx = nodeIdx.incrementAndGet();

                        IgniteConfiguration cfg;

                        switch (ThreadLocalRandom.current().nextInt(0, 3)) {
                            case 1:
                                cfg = serverConfiguration(lastIdx, false);

                                break;

                            case 2:

                                cfg = serverConfiguration(lastIdx, true);

                                break;

                            default:
                                cfg = clientConfiguration(lastIdx);
                        }

                        ignitionStart(cfg);

                        exists = true;
                    }

                    Thread.sleep(ThreadLocalRandom.current().nextLong(500L, 1500L));
                }

                return null;
            }
        }, 1);

        final GridConcurrentHashSet<Integer> fields = new GridConcurrentHashSet<>();

        IgniteInternalFuture idxFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    int fieldNum = ThreadLocalRandom.current().nextInt(0, dynColCnt.get() + 1);

                    boolean removed = fields.remove(fieldNum);

                    if (removed)
                        fut = dropCols(node, QueryUtils.DFLT_SCHEMA, "newCol" + fieldNum);
                    else {
                        fieldNum = dynColCnt.getAndIncrement();

                        fut = addCols(node, QueryUtils.DFLT_SCHEMA, c("newCol" + fieldNum, Integer.class.getName()));
                    }

                    try {
                        fut.get();

                        if (!removed)
                            fields.add(fieldNum);
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 1);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        startStopFut.get();
        idxFut.get();

        // Make sure cache is operational at this point.
        createSqlCache(cli);

        QueryField[] expCols = new QueryField[fields.size()];

        // Too many index columns kills indexing internals, have to limit number of the columns
        // to build the index on.
        int idxColsCnt = Math.min(300, expCols.length);

        Integer[] args = new Integer[idxColsCnt];

        String updQry = "UPDATE " + TBL_NAME + " SET ";

        String idxQry = "CREATE INDEX idx ON " + TBL_NAME + '(';

        Integer[] sorted = fields.toArray(new Integer[fields.size()]);

        Arrays.sort(sorted);

        for (int i = 0; i < expCols.length; i++) {
            int fieldNum = sorted[i];

            expCols[i] = c("newCol" + fieldNum, Integer.class.getName());

            if (i >= idxColsCnt)
                continue;

            if (i > 0) {
                updQry += ", ";

                idxQry += ", ";
            }

            updQry += "\"newCol" + fieldNum + "\" = id + ?";

            idxQry += "\"newCol" + fieldNum + '"';

            args[i] = i;
        }

        idxQry += ')';

        checkTableState(cli, QueryUtils.DFLT_SCHEMA, TBL_NAME, expCols);

        put(cli, 0, 500);

        run(cli.cache(CACHE_NAME), updQry, (Object[])args);

        run(cli, idxQry);

        run(cli, "DROP INDEX idx");
    }

    /**
     * Block indexing.
     *
     * @param node Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private static CountDownLatch blockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        return blockIndexing(nodeId);
    }

    /**
     * Block indexing.
     *
     * @param nodeId Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private static CountDownLatch blockIndexing(UUID nodeId) {
        assertFalse(BLOCKS.contains(nodeId));

        CountDownLatch idxLatch = new CountDownLatch(1);

        BLOCKS.put(nodeId, new T3<>(new CountDownLatch(1), new AtomicBoolean(), idxLatch));

        return idxLatch;
    }

    /**
     * Unblock indexing.
     *
     * @param node Node.
     */
    private static void unblockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        unblockIndexing(nodeId);
    }

    /**
     * Unblock indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    private static void unblockIndexing(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.remove(nodeId);

        assertNotNull(blocker);

        blocker.get1().countDown();
    }

    /**
     * Await indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    private static void awaitIndexing(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.get(nodeId);

        if (blocker != null) {
            assertTrue(blocker.get2().compareAndSet(false, true));

            blocker.get3().countDown();

            while (true) {
                try {
                    blocker.get1().await();

                    break;
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        }
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols,
            boolean ifTblExists, boolean ifColNotExists)
            throws IgniteCheckedException {
            awaitIndexing(ctx.localNodeId());

            super.dynamicAddColumn(schemaName, tblName, cols, ifTblExists, ifColNotExists);
        }

        /** {@inheritDoc} */
        @Override public void dynamicDropColumn(String schemaName, String tblName, List<String> cols,
            boolean ifTblExists, boolean ifColExists) throws IgniteCheckedException {
            awaitIndexing(ctx.localNodeId());

            super.dynamicDropColumn(schemaName, tblName, cols, ifTblExists, ifColExists);
        }
    }

    /**
     *
     * @param node Target node.
     * @param schemaName Schema name.
     * @param flds Columns to add.
     * @return DDL operation future.
     */
    private static IgniteInternalFuture<?> addCols(Ignite node, String schemaName, QueryField... flds) {
        final String cacheName = F.eq(schemaName, QueryUtils.DFLT_SCHEMA) ? CACHE_NAME : "idx";

        return ((IgniteEx)node).context().query().dynamicColumnAdd(cacheName, schemaName, TBL_NAME,
            Arrays.asList(flds), false, false);
    }

    /**
     *
     * @param node Target node.
     * @param schemaName Schema name.
     * @param flds Columns to remove.
     * @return DDL operation future.
     */
    private static IgniteInternalFuture<?> dropCols(Ignite node, String schemaName, String... flds) {
        final String cacheName = F.eq(schemaName, QueryUtils.DFLT_SCHEMA) ? CACHE_NAME : "idx";

        return ((IgniteEx)node).context().query().dynamicColumnRemove(cacheName, schemaName, TBL_NAME,
            Arrays.asList(flds), false, false);
    }

    /**
     * Start SQL cache on given node.
     * @param node Node to create cache on.
     * @return Created cache.
     */
    private IgniteCache<?, ?> createSqlCache(Ignite node) throws IgniteCheckedException {
        node.addCacheConfiguration(new CacheConfiguration<>("TPL")
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setNodeFilter(new NodeFilter()));

        return node.getOrCreateCache(new CacheConfiguration<>("idx").setIndexedTypes(Integer.class, Integer.class));
    }

    /**
     * Spoof blocking indexing class and start new node.
     * @param cfg Node configuration.
     * @return New node.
     */
    private static IgniteEx ignitionStart(IgniteConfiguration cfg) {
        return ignitionStart(cfg, null);
    }

    /**
     * Spoof blocking indexing class and start new node.
     * @param cfg Node configuration.
     * @param latch Latch to await for ultimate completion of DDL operations.
     * @return New node.
     */
    private static IgniteEx ignitionStart(IgniteConfiguration cfg, final CountDownLatch latch) {
        // Have to do this for each starting node - see GridQueryProcessor ctor, it nulls
        // idxCls static field on each call.
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx node = (IgniteEx)Ignition.start(cfg);

        if (latch != null)
            node.context().discovery().setCustomEventListener(SchemaFinishDiscoveryMessage.class,
                new CustomEventListener<SchemaFinishDiscoveryMessage>() {
                    @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                        SchemaFinishDiscoveryMessage msg) {
                        latch.countDown();
                    }
                });

        return node;
    }

    /**
     * Create server configuration.
     * @param nodeIdx Node index.
     * @param filtered Whether this node should not be treated as affinity node.
     * @return Configuration.
     * @throws Exception if failed.
     */
    private IgniteConfiguration serverConfiguration(int nodeIdx, boolean filtered) throws Exception {
        IgniteConfiguration cfg = serverConfiguration(nodeIdx);

        if (filtered)
            cfg.setUserAttributes(Collections.singletonMap(ATTR_FILTERED, true));

        return cfg;
    }

    /**
     * Runnable which can throw checked exceptions.
     */
    interface RunnableX {
        /**
         * Do run.
         *
         * @throws Exception If failed.
         */
        @SuppressWarnings("UnnecessaryInterfaceModifier")
        public void run() throws Exception;
    }

    /**
     * Node filter.
     */
    protected static class NodeFilter implements IgnitePredicate<ClusterNode>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(ATTR_FILTERED) == null;
        }
    }
}

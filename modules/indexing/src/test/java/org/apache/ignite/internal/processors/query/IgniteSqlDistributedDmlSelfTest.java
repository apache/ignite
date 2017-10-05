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

package org.apache.ignite.internal.processors.query;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;

/** Tests for distributed DML. */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class IgniteSqlDistributedDmlSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static int NODE_COUNT = 4;

    /** */
    private static String NODE_CLIENT = "client";

    /** */
    private static String CACHE_ORG = "org";

    /** */
    private static String CACHE_PERSON = "person";

    /** */
    private static String CACHE_PERSON2 = "person-2";

    /** */
    private static String CACHE_POSITION = "pos";

    /** */
    private static Ignite client;

    /** */
    private static CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(buildCacheConfiguration(CACHE_ORG));
        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_POSITION));

        ccfgs.add(buildCacheConfiguration(CACHE_PERSON).setName(CACHE_PERSON2));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        if (gridName.equals(NODE_CLIENT))
            c.setClientMode(true);

        return c;
    }

    /** */
    private CacheConfiguration buildCacheConfiguration(String name) {
        if (name.equals(CACHE_ORG)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_ORG);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, Organization.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setSqlFunctionClasses(IgniteSqlDistributedDmlSelfTest.class);

            return ccfg;
        }
        if (name.equals(CACHE_PERSON)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_PERSON);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(PersonKey.class, Person.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setKeyConfiguration(new CacheKeyConfiguration(PersonKey.class));

            ccfg.setSqlFunctionClasses(IgniteSqlDistributedDmlSelfTest.class);

            return ccfg;
        }
        if (name.equals(CACHE_POSITION)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_POSITION);

            ccfg.setCacheMode(CacheMode.REPLICATED);

            QueryEntity entity = new QueryEntity(Integer.class, Position.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setSqlFunctionClasses(IgniteSqlDistributedDmlSelfTest.class);

            return ccfg;
        }

        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        client = startGrid(NODE_CLIENT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        checkNoLeaks();

        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Ignite client = grid(NODE_CLIENT);

        // Stop additional node that is started in one of the test.
        stopGrid(NODE_COUNT + 1);

        awaitPartitionMapExchange();

        client.cache(CACHE_PERSON).clear();
        client.cache(CACHE_PERSON2).clear();
        client.cache(CACHE_ORG).clear();
        client.cache(CACHE_POSITION).clear();
    }

    // PARTITIONED Integer -> Organization (name, int rate, updated)
    // PARTITIONED PersonKey(orgId, id) -> Person (name, int position, amount, updated)
    // REPLICATED Position (id, name, int rate)
    /** */
    public void testUpdate() throws Exception {
        fillCaches();

        String text = "UPDATE \"person\".Person SET amount = amount * 2 WHERE amount > 0";

        checkUpdate(new SqlFieldsQuery(text), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testUpdateFastKey() throws Exception {
        fillCaches();

        String text = "UPDATE \"person\".Person SET amount = ? WHERE orgId = ? and id = ?";

        checkUpdate(new SqlFieldsQuery(text).setArgs(100, 1, 1), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testUpdateLimit() throws Exception {
        fillCaches();

        // UPDATE LIMIT can't produce stable/repeatable results (and UPDATE ORDER BY is ignored by H2)
        // So we must first get the count and use it later as limit
        List<List<?>> r = grid(NODE_CLIENT).context().query().querySqlFieldsNoCache(
            new SqlFieldsQuery("SELECT COUNT(*) FROM \"person\".Person WHERE position = ?").setArgs(1), false).getAll();

        int count = ((Number)r.get(0).get(0)).intValue();

        String text = "UPDATE \"person\".Person SET amount = amount * 2  WHERE position = ? LIMIT ?";

        checkUpdate(new SqlFieldsQuery(text).setArgs(1, count), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testUpdateWhereSubquery() throws Exception {
        fillCaches();

        String text = "UPDATE \"person\".Person SET amount = amount * 2 " +
            "WHERE amount > 0 AND position IN (SELECT p._key FROM \"pos\".Position p WHERE rate > 3)";

        checkUpdate(new SqlFieldsQuery(text), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testUpdateSetSubquery() throws Exception {
        fillCaches();

        String text = "UPDATE \"person\".Person p SET amount = " +
            "(SELECT o.rate FROM \"pos\".Position o WHERE p.position = o._key)";

        checkUpdate(new SqlFieldsQuery(text), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testUpdateSetTableSubquery() throws Exception {
        fillCaches();

        String text = "UPDATE \"person\".Person p SET (amount, updated) = " +
            "(SELECT o.rate, CURDATE() FROM \"pos\".Position o WHERE p.position = o._key)";

        checkUpdate(new SqlFieldsQuery(text), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    // PARTITIONED Integer -> Organization (name, int rate, updated)
    // PARTITIONED PersonKey(orgId, id) -> Person (name, int position, amount, updated)
    // REPLICATED Position (id, name, int rate)
    //
    /** */
    public void testInsertValues() throws Exception {
        String sqlText = "INSERT INTO \"person\".Person () VALUES ()";
    }

    /** */
    public void testInsertFromSelect() throws Exception {
        // INSERT INTO tblName (fields..) SELECT
    }

    /** */
    public void testInsertFromSelectJoin() throws Exception {
        // INSERT INTO tblName (fields..) SELECT JOIN
    }

    /** */
    public void testInsertFromSelectOrderBy() throws Exception {
        // INSERT INTO tblName (fields..) SELECT ORDER BY
    }

    /** */
    public void testInsertFromSelectGroupBy() throws Exception {
        // INSERT INTO tblName (fields..) SELECT GROUP BY
    }

    /** */
    public void testInsertFromSelectSubquery() throws Exception {
        // INSERT INTO tblName (fields..) SELECT WHERE SUB-QUERY
    }

    /** */
    public void testInsertFromSelectLimitOffset() throws Exception {
        // TODO: OFFSET/LIMIT can't provide stable/repeatable results
        // INSERT INTO tblName (fields..) SELECT OFFSET LIMIT
    }

    /** */
    public void testInsertFromSelectDistinct() throws Exception {
        // INSERT INTO tblName (fields..) SELECT DISTINCT
    }

    /** */
    public void testInsertFromSelectUnion() throws Exception {
        // INSERT INTO tblName (fields..) SELECT UNION SELECT
    }

    /** */
    public void testInsertSetField() throws Exception {
        // INSERT INTO tblName (fields..) SET field = ?, field2 = ?
    }

    /** */
    public void testInsertSetFieldSubquery() throws Exception {
        // INSERT INTO tblName (fields..) SET field = (SUB-QUERY), field2 = (SUB-QUERY)
    }

    // PARTITIONED Integer -> Organization (name, int rate, updated)
    // PARTITIONED PersonKey(orgId, id) -> Person (name, int position, amount, updated)
    // REPLICATED Position (id, name, int rate)
    /** */
    public void testMergeValues() throws Exception {
        // MERGE INTO tblName (fields..) VALUES (vals..)
    }

    /** */
    public void testMergeFromSelectJoin() throws Exception {
        // MERGE INTO tblName (fields..) SELECT JOIN
    }

    /** */
    public void testMergeFromSelectOrderBy() throws Exception {
        // MERGE INTO tblName (fields..) SELECT ORDER BY
    }

    /** */
    public void testMergeFromSelectGroupBy() throws Exception {
        // MERGE INTO tblName (fields..) SELECT GROUP BY
    }

    /** */
    public void testMergeFromSelectDistinct() throws Exception {
        // MERGE INTO tblName (fields..) SELECT DISTINCT
    }

    /** */
    public void testMergeFromSelectLimitOffset() throws Exception {
        // TODO: OFFSET/LIMIT can't provide stable/repeatable results
        // MERGE INTO tblName (fields..) SELECT OFFSET LIMIT
    }

    /** */
    public void testMergeFromSelectUnion() throws Exception {
        // MERGE INTO tblName (fields..) SELECT UNION SELECT
    }

    /** */
    public void testMergeFromSelectSubquery() throws Exception {
        // MERGE INTO tblName (fields..) SELECT WHERE SUBQUERY
    }

    // PARTITIONED Integer -> Organization (name, int rate, updated)
    // PARTITIONED PersonKey(orgId, id) -> Person (name, int position, amount, updated)
    // REPLICATED Position (id, name, int rate)
    /** */
    public void testDelete() throws Exception {
        fillCaches();

        String text = "DELETE FROM \"person\".Person WHERE position = ?";

        checkUpdate(new SqlFieldsQuery(text).setArgs(1), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testDeleteTop() throws Exception {
        fillCaches();
        // TOP/LIMIT can't provide stable/repeatable results, so we need to get count first to use it as limit
        List<List<?>> r = grid(NODE_CLIENT).context().query().querySqlFieldsNoCache(
            new SqlFieldsQuery("SELECT COUNT(*) FROM \"person\".Person WHERE position = ?").setArgs(1), false).getAll();

        int count = ((Number)r.get(0).get(0)).intValue();

        String text = "DELETE TOP ? FROM \"person\".Person WHERE position = ?";

        checkUpdate(new SqlFieldsQuery(text).setArgs(count, 1), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testDeleteWhereSubquery() throws Exception {
        fillCaches();

        String text = "DELETE FROM \"person\".Person " +
            "WHERE position IN (SELECT o._key FROM \"pos\".Position o WHERE rate > 3)";

        checkUpdate(new SqlFieldsQuery(text), CACHE_PERSON, CACHE_PERSON2);

        compareTablesContent(CACHE_PERSON, CACHE_PERSON2, "Person");
    }

    /** */
    public void testSimpleUpdateDistributedReplicated() throws Exception {
        fillCaches();

        IgniteCache<Integer, Position> cache = grid(NODE_CLIENT).cache(CACHE_POSITION);

        Position p = cache.get(1);

        List<List<?>> r = cache.query(new SqlFieldsQuery("UPDATE Position p SET name = CONCAT('A ', name)")
            .setUpdateOnServer(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));

        assertEquals(cache.get(1).name, "A " + p.name);
    }

    /** */
    public void testSimpleUpdateDistributedPartitioned() throws Exception {
        fillCaches();

        IgniteCache<PersonKey, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        List<List<?>> r = cache.query(new SqlFieldsQuery(
            "UPDATE Person SET position = CASEWHEN(position = 1, 1, position - 1)")
            .setUpdateOnServer(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));
    }

    /** */
    public void testDistributedUpdateFailedKeys() throws Exception {
        // UPDATE can produce failed keys due to concurrent modification
        fillCaches();

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQuery("UPDATE Organization SET rate = Modify(_key, rate - 1)")
                    .setUpdateOnServer(true));
            }
        }, CacheException.class, "Failed to update some keys because they had been modified concurrently");
    }

    /** */
    public void testDistributedUpdateFail() throws Exception {
        fillCaches();

        final IgniteCache cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQuery("UPDATE Person SET name = Fail(name)")
                    .setUpdateOnServer(true));
            }
        }, CacheException.class, "Failed to execute SQL query");
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    public void testQueryParallelism() throws Exception {
        String cacheName = CACHE_ORG + "x4";

        CacheConfiguration cfg = buildCacheConfiguration(CACHE_ORG)
            .setQueryParallelism(4)
            .setName(cacheName);

        IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).createCache(cfg);

        for (int i = 0; i < 1024; i++)
            cache.put(i, new Organization("Acme Inc #" + i, 0));

        List<List<?>> r = cache.query(new SqlFieldsQuery("UPDATE \"" + cacheName +
            "\".Organization o SET name = UPPER(name)").setUpdateOnServer(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));
    }

    /** */
    public void testEvents() throws Exception {
        final CountDownLatch latch = new CountDownLatch(NODE_COUNT);

        final IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CacheQueryExecutedEvent;

                CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                assertNotNull(qe.clause());

                latch.countDown();

                return true;
            }
        };

        for (int idx = 0; idx < NODE_COUNT; idx++)
            grid(idx).events().localListen(pred, EVT_CACHE_QUERY_EXECUTED);

        IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        for (int i = 0; i < 1024; i++)
            cache.put(i, new Organization("Acme Inc #" + i, 0));

        cache.query(new SqlFieldsQuery("UPDATE \"org\".Organization o SET name = UPPER(name)")
            .setUpdateOnServer(true)).getAll();

        assertTrue(latch.await(5000, MILLISECONDS));

        for (int idx = 0; idx < NODE_COUNT; idx++)
            grid(idx).events().stopLocalListen(pred);
    }

    /** */
    public void testSpecificPartitionsUpdate() throws Exception {
        fillCaches();

        Affinity aff = grid(NODE_CLIENT).affinity(CACHE_PERSON);

        int numParts = aff.partitions();
        int parts[] = new int[numParts / 2];

        for (int idx = 0; idx < numParts / 2; idx++)
            parts[idx] = idx * 2;

        IgniteCache<PersonKey, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        // UPDATE over even partitions
        cache.query(new SqlFieldsQuery("UPDATE Person SET position = 0")
                .setUpdateOnServer(true)
                .setPartitions(parts));

        List<List<?>> rows = cache.query(new SqlFieldsQuery("SELECT _key, position FROM Person")).getAll();

        for (List<?> row : rows) {
            PersonKey personKey = (PersonKey)row.get(0);
            int pos = ((Number)row.get(1)).intValue();
            int part = aff.partition(personKey);

            assertTrue((part % 2 == 0) ^ (pos != 0));
        }
    }

    /** */
    public void testCancel() throws Exception {
        fillCaches();

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQuery("UPDATE Organization SET name = WAIT(name)")
                    .setUpdateOnServer(true));
            }
        });

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Collection<GridRunningQueryInfo> qCol =
                    grid(NODE_CLIENT).context().query().runningQueries(0);

                if (qCol.isEmpty())
                    return false;

                for (GridRunningQueryInfo queryInfo : qCol)
                    queryInfo.cancel();

                return true;
            }
        }, 5000);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return fut.get();
            }
        }, IgniteCheckedException.class, "Future was cancelled");
    }

    /** */
    public void testNodeStopDuringUpdate() throws Exception {
        startGrid(NODE_COUNT + 1);

        awaitPartitionMapExchange();

        fillCaches();

        latch = new CountDownLatch(NODE_COUNT + 1);

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQuery("UPDATE Organization SET name = COUNTANDWAIT(name)")
                    .setUpdateOnServer(true));
            }
        });

        assertTrue(latch.await(5000, MILLISECONDS));

        stopGrid(NODE_COUNT + 1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return fut.get();
            }
        }, IgniteCheckedException.class, "Update failed because map node left topology");
    }

    /**
     * Ensure there are no leaks in data structures associated with distributed dml execution.
     */
    private void checkNoLeaks() {
        GridQueryProcessor qryProc = grid(NODE_CLIENT).context().query();

        IgniteH2Indexing h2Idx = GridTestUtils.getFieldValue(qryProc, GridQueryProcessor.class, "idx");

        GridReduceQueryExecutor rdcQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "rdcQryExec");

        Map updRuns = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "updRuns");

        assertEquals(0, updRuns.size());

        for (int idx = 0; idx < NODE_COUNT; idx++) {
            qryProc = grid(idx).context().query();

            h2Idx = GridTestUtils.getFieldValue(qryProc, GridQueryProcessor.class, "idx");

            GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

            Map qryRess = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "qryRess");

            for (Object obj : qryRess.values()) {
                Map updCancels = GridTestUtils.getFieldValue(obj, "updCancels");

                assertEquals(0, updCancels.size());
            }
        }
    }

    /** */
    private void checkUpdate(SqlFieldsQuery updQry, String cacheNameA, String cacheNameB) {
        GridQueryProcessor queryProc = grid(NODE_CLIENT).context().query();

        List<List<?>> r1 = queryProc.querySqlFieldsNoCache(new SqlFieldsQuery(updQry)
            .setUpdateOnServer(true), true).getAll();

        List<List<?>> r2 = queryProc.querySqlFieldsNoCache(new SqlFieldsQuery(updQry)
            .setSql(updQry.getSql().replace(cacheNameA, cacheNameB))
            .setUpdateOnServer(false), true).getAll();

        assertNotNull(r1);
        assertNotNull(r2);

        assertEquals(1, r1.size());
        assertEquals(1, r2.size());

        assertEquals(r1.get(0).get(0), r2.get(0).get(0));

        assertTrue(((Number)r1.get(0).get(0)).intValue() > 0);
    }

    /** */
    private void compareTablesContent(String cacheNameA, String cacheNameB, String tableName) throws Exception {
        IgniteEx client = grid(NODE_CLIENT);

        GridQueryProcessor queryProc = client.context().query();

        assertEquals(client.cache(cacheNameA).size(), client.cache(cacheNameB).size());

        int size = client.cache(cacheNameA).size();

        String sqlText = "SELECT COUNT(*) FROM \"" + cacheNameA + "\"." + tableName +
            " a JOIN \"" + cacheNameB + "\"." + tableName + " b ON a._key = b._key " +
            "WHERE a._val = b._val";

        List<List<?>> r = queryProc.querySqlFieldsNoCache(new SqlFieldsQuery(sqlText), true).getAll();

        assertNotNull(r);

        assertEquals(1, r.size());

        assertEquals(size, ((Number)r.get(0).get(0)).intValue());
    }

    /** */
    private void checkResults(List<List<?>> a, List<List<?>> b) {
        assertNotNull(a);
        assertNotNull(b);

        assertEquals(a.size(), b.size());

        assertTrue(a.size() > 0);

        int sz = a.size();
        for (int i = 0; i < sz; ++i) {
            List<?> ra = a.get(i);
            List<?> rb = b.get(i);

            assertNotNull(ra);
            assertNotNull(rb);

            assertEquals(ra.size(), rb.size());

            assertTrue(ra.size() > 0);

            int rSize = ra.size();

            for (int j = 0; j < rSize; ++j)
                assertEquals(ra.get(j), rb.get(j));
        }
    }

    /** */
    private void fillCaches() {
        Ignite client = grid(NODE_CLIENT);

        IgniteCache<Integer, Position> posCache = client.cache(CACHE_POSITION);

        // Generate positions
        Position[] positions = new Position[] {
            new Position(1, "High Ranking Officer", 1),
            new Position(2, "Administrative worker", 3),
            new Position(3, "Worker", 7),
            new Position(4, "Security", 2),
            new Position(5, "Cleaner", 1)
        };

        for (Position pos: positions)
            posCache.put(pos.id, pos);

        // Generate organizations
        String[] forms = new String[] {" Inc", " Co", " AG", " Industries"};
        String[] orgNames = new String[] {"Acme", "Sierra", "Mesa", "Umbrella", "Robotics"};
        String[] names = new String[] {"Mary", "John", "William", "Tom", "Basil", "Ann", "Peter"};

        IgniteCache<PersonKey, Person> personCache = client.cache(CACHE_PERSON);
        IgniteCache<PersonKey, Person> personCache2 = client.cache(CACHE_PERSON2);

        IgniteCache<Integer, Organization> orgCache = client.cache(CACHE_ORG);

        int orgId = 0;
        int personId = 0;

        for (String orgName : produceCombination(orgNames, orgNames, forms)) {
            Organization org = new Organization(orgName, 1 + orgId);

            orgCache.put(++orgId, org);

            // Generate persons

            List<String> personNames = produceCombination(names, names, new String[]{"s"});

            int positionId = 0;
            int posCounter = 0;

            for (String name : personNames) {
                PersonKey pKey = new PersonKey(orgId, ++personId);

                if (positions[positionId].rate < posCounter++) {
                    posCounter = 0;
                    positionId = (positionId + 1) % positions.length;
                }

                Person person = new Person(name, positions[positionId].id, org.rate * positions[positionId].rate);

                personCache.put(pKey, person);
                personCache2.put(pKey, person);
            }
        }
    }

    /** */
    private List<String> produceCombination(String[] a, String[] b, String[] ends) {
        List<String> res = new ArrayList<>();

        for (String s1 : a) {
            for (String s2 : b) {
                if (!s1.equals(s2)) {
                    String end = ends[ThreadLocalRandom8.current().nextInt(ends.length)];

                    res.add(s1 + " " + s2 + end);
                }
            }
        }

        return res;
    }

    /** */
    private static class Organization {
        /** */
        @QuerySqlField
        String name;

        @QuerySqlField
        int rate;

        @QuerySqlField
        Date updated;

        /** */
        public Organization() {
            // No-op.
        }

        /**
         * @param name Organization name.
         */
        public Organization(String name, int rate) {
            this.name = name;
            this.rate = rate;
            this.updated = new Date(System.currentTimeMillis());
        }
    }

    /** */
    public static class PersonKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private Integer orgId;

        /** */
        @QuerySqlField
        private Integer id;

        /** */
        PersonKey(int orgId, int id) {
            this.orgId = orgId;
            this.id = id;
        }
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField
        String name;

        @QuerySqlField
        int position;

        @QuerySqlField
        int amount;

        @QuerySqlField
        Date updated;

        /** */
        private Person(String name, int position, int amount) {
            this.name = name;
            this.position = position;
            this.amount = amount;
            this.updated = new Date(System.currentTimeMillis());
        }

        @Override public int hashCode() {
            return (name==null? 0: name.hashCode()) ^ position ^ amount ^ (updated == null ? 0 : updated.hashCode());
        }

        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!obj.getClass().equals(Person.class))
                return false;

            Person other = (Person)obj;

            return F.eq(name, other.name) && position == other.position &&
                amount == other.amount && F.eq(updated, other.updated);
        }

    }

    /** */
    private static class Position {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        int rate;

        /** */
        public Position(int id, String name, int rate) {
            this.id = id;
            this.name = name;
            this.rate = rate;
        }
    }

    /** */
    @QuerySqlFunction
    public static String Fail(String param) {
        throw new IgniteSQLException("Fail() called");
    }

    /** */
    @QuerySqlFunction
    public static String Wait(String param) {
        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException ignore) {
            // No-op
        }
        return param;
    }

    /** */
    @QuerySqlFunction
    public static String CountAndWait(String param) {
        try {
            latch.countDown();

            Thread.sleep(3000);
        }
        catch (InterruptedException ignore) {
            // No-op
        }
        return param;
    }

    /** */
    @QuerySqlFunction
    public static int Modify(final int id, final int rate) {
        try {
            GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() {
                    IgniteCache cache = client.cache(CACHE_ORG);

                    cache.put(id, new Organization("Acme Inc #" + id, rate + 1));

                    return null;
                }
            }).get();
        }
        catch (Exception e) {
            // No-op
        }

        return rate - 1;
    }
}

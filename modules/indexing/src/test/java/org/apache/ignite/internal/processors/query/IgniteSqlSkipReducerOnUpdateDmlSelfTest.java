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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
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
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;

/**
 * Tests for distributed DML.
 */
@SuppressWarnings({"unchecked"})
public class IgniteSqlSkipReducerOnUpdateDmlSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODE_COUNT = 4;

    /** */
    private static final String NODE_CLIENT = "client";

    /** */
    private static final String CACHE_ORG = "org";

    /** */
    private static final String CACHE_PERSON = "person";

    /** */
    private static final String CACHE_POSITION = "pos";

    /** */
    private static Ignite client;

    /** */
    private static CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(buildCacheConfiguration(CACHE_ORG));
        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_POSITION));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));
        c.setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(10000));
        c.setIncludeEventTypes(EventType.EVTS_ALL);

        return c;
    }

    /**
     * Creates cache configuration.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration buildCacheConfiguration(String name) {
        if (name.equals(CACHE_ORG)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_ORG);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, Organization.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setSqlFunctionClasses(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class);

            return ccfg;
        }
        if (name.equals(CACHE_PERSON)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_PERSON);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(PersonKey.class, Person.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setKeyConfiguration(new CacheKeyConfiguration(PersonKey.class));

            ccfg.setSqlFunctionClasses(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class);

            return ccfg;
        }
        if (name.equals(CACHE_POSITION)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_POSITION);

            ccfg.setCacheMode(CacheMode.REPLICATED);

            QueryEntity entity = new QueryEntity(Integer.class, Position.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            ccfg.setSqlFunctionClasses(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class);

            return ccfg;
        }

        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        client = startClientGrid(NODE_CLIENT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        checkNoLeaks();

        client = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Stop additional node that is started in one of the test.
        stopGrid(NODE_COUNT + 1);

        awaitPartitionMapExchange();

        client.cache(CACHE_PERSON).clear();
        client.cache(CACHE_ORG).clear();
        client.cache(CACHE_POSITION).clear();
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSimpleUpdateDistributedReplicated() throws Exception {
        fillCaches();

        IgniteCache<Integer, Position> cache = grid(NODE_CLIENT).cache(CACHE_POSITION);

        Position p = cache.get(1);

        List<List<?>> r = cache.query(new SqlFieldsQueryEx("UPDATE Position p SET name = CONCAT('A ', name)", false)
            .setSkipReducerOnUpdate(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));

        assertEquals(cache.get(1).name, "A " + p.name);
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSimpleUpdateDistributedPartitioned() throws Exception {
        fillCaches();

        IgniteCache<PersonKey, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        List<List<?>> r = cache.query(new SqlFieldsQueryEx(
            "UPDATE Person SET position = CASEWHEN(position = 1, 1, position - 1)", false)
            .setSkipReducerOnUpdate(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDistributedUpdateFailedKeys() throws Exception {
        // UPDATE can produce failed keys due to concurrent modification
        fillCaches();

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQueryEx("UPDATE Organization SET rate = Modify(_key, rate - 1)", false)
                    .setSkipReducerOnUpdate(true));
            }
        }, CacheException.class, "Failed to update some keys because they had been modified concurrently");
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDistributedUpdateFail() throws Exception {
        fillCaches();

        final IgniteCache cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQueryEx("UPDATE Person SET name = Fail(name)", false)
                    .setSkipReducerOnUpdate(true));
            }
        }, CacheException.class, "Failed to run SQL update query.");
    }

    /**
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testQueryParallelism() throws Exception {
        String cacheName = CACHE_ORG + "x4";

        CacheConfiguration cfg = buildCacheConfiguration(CACHE_ORG)
            .setQueryParallelism(4)
            .setName(cacheName);

        IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).createCache(cfg);

        for (int i = 0; i < 1024; i++)
            cache.put(i, new Organization("Acme Inc #" + i, 0));

        List<List<?>> r = cache.query(new SqlFieldsQueryEx("UPDATE \"" + cacheName +
            "\".Organization o SET name = UPPER(name)", false).setSkipReducerOnUpdate(true)).getAll();

        assertEquals((long)cache.size(), r.get(0).get(0));
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
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

        cache.query(new SqlFieldsQueryEx("UPDATE \"org\".Organization o SET name = UPPER(name)", false)
            .setSkipReducerOnUpdate(true)).getAll();

        assertTrue(latch.await(5000, MILLISECONDS));

        for (int idx = 0; idx < NODE_COUNT; idx++)
            grid(idx).events().stopLocalListen(pred);
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSpecificPartitionsUpdate() throws Exception {
        fillCaches();

        Affinity aff = grid(NODE_CLIENT).affinity(CACHE_PERSON);

        int numParts = aff.partitions();
        int parts[] = new int[numParts / 2];

        for (int idx = 0; idx < numParts / 2; idx++)
            parts[idx] = idx * 2;

        IgniteCache<PersonKey, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        // UPDATE over even partitions
        cache.query(new SqlFieldsQueryEx("UPDATE Person SET position = 0", false)
                .setSkipReducerOnUpdate(true)
                .setPartitions(parts));

        List<List<?>> rows = cache.query(new SqlFieldsQuery("SELECT _key, position FROM Person")).getAll();

        for (List<?> row : rows) {
            PersonKey personKey = (PersonKey)row.get(0);
            int pos = ((Number)row.get(1)).intValue();
            int part = aff.partition(personKey);

            assertTrue((part % 2 == 0) ^ (pos != 0));
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testCancel() throws Exception {
        latch = new CountDownLatch(NODE_COUNT + 1);

        fillCaches();

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQueryEx("UPDATE Organization SET name = WAIT(name)", false)
                    .setSkipReducerOnUpdate(true));
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

        latch.await(5000, MILLISECONDS);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return fut.get();
            }
        }, IgniteCheckedException.class, "Future was cancelled");
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void testNodeStopDuringUpdate() throws Exception {
        startGrid(NODE_COUNT + 1);

        awaitPartitionMapExchange();

        fillCaches();

        latch = new CountDownLatch(NODE_COUNT + 1 + 1);

        final IgniteCache<Integer, Organization> cache = grid(NODE_CLIENT).cache(CACHE_ORG);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                return cache.query(new SqlFieldsQueryEx("UPDATE Organization SET name = WAIT(name)", false)
                    .setSkipReducerOnUpdate(true));
            }
        });

        final CountDownLatch finalLatch = latch;

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return finalLatch.getCount() == 1;
            }
        }, 5000));

        latch.countDown();

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

    /**
     * Fills caches with initial data.
     */
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
            }
        }
    }

    /**
     * Produces all possible combinations.
     *
     * @param a First array.
     * @param b Second array.
     * @param ends Endings array.
     * @return Result.
     */
    private List<String> produceCombination(String[] a, String[] b, String[] ends) {
        List<String> res = new ArrayList<>();

        for (String s1 : a) {
            for (String s2 : b) {
                if (!s1.equals(s2)) {
                    String end = ends[ThreadLocalRandom.current().nextInt(ends.length)];

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

        /** */
        @QuerySqlField
        int rate;

        /** */
        @QuerySqlField
        Date updated;

        /**
         * Constructor.
         *
         * @param name Organization name.
         * @param rate Rate.
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

        /**
         * Constructor.
         *
         * @param orgId Organization id.
         * @param id Person id.
         */
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

        /** */
        @QuerySqlField
        int position;

        /** */
        @QuerySqlField
        int amount;

        /** */
        @QuerySqlField
        Date updated;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param position Position.
         * @param amount Amount.
         */
        private Person(String name, int position, int amount) {
            this.name = name;
            this.position = position;
            this.amount = amount;

            this.updated = new Date(System.currentTimeMillis());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (name == null ? 0 : name.hashCode()) ^ position ^ amount ^ (updated == null ? 0 : updated.hashCode());
        }

        /** {@inheritDoc} */
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

        /**
         * Constructor.
         *
         * @param id Id.
         * @param name Name.
         * @param rate Rate.
         */
        public Position(int id, String name, int rate) {
            this.id = id;
            this.name = name;
            this.rate = rate;
        }
    }

    /**
     * SQL function that always fails.
     *
     * @param param Arbitrary parameter.
     * @return Result.
     */
    @QuerySqlFunction
    public static String Fail(String param) {
        throw new IgniteSQLException("Fail() called");
    }

    /**
     * SQL function that waits for condition.
     *
     * @param param Arbitrary parameter.
     * @return Result.
     */
    @QuerySqlFunction
    public static String Wait(String param) {
        try {
            if (latch.getCount() > 0) {
                latch.countDown();

                latch.await(5000, MILLISECONDS);
            }
            else
                Thread.sleep(100);
        }
        catch (InterruptedException ignore) {
            // No-op
        }
        return param;
    }

    /**
     * SQL function that makes a concurrent modification.
     *
     * @param id Id.
     * @param rate Rate.
     * @return Result.
     */
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

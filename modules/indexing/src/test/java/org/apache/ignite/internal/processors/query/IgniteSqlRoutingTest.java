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

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;

/** Tests for query partitions derivation. */
public class IgniteSqlRoutingTest extends AbstractIndexingCommonTest {
    /** */
    private static final String NODE_CLIENT = "client";

    /** */
    private static final String CACHE_PERSON = "Person";

    /** */
    private static final String CACHE_CALL = "Call";

    /** */
    private static final int NODE_COUNT = 4;

    /** Broadcast query to ensure events came from all nodes. */
    private static final String FINAL_QRY = "select count(1) from {0} where name=?";

    /** Param to distinguish the final query event. */
    private static final String FINAL_QRY_PARAM = "Abracadabra";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshaller(new BinaryMarshaller());

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        CacheConfiguration ccfg = buildCacheConfiguration(gridName);

        if (ccfg != null)
            ccfgs.add(ccfg);

        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_CALL));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));
        c.setCacheKeyConfiguration(new CacheKeyConfiguration(CallKey.class));
        c.setIncludeEventTypes(EventType.EVTS_ALL);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        startClientGrid(NODE_CLIENT);

        awaitPartitionMapExchange();

        fillCaches();
    }

    /** */
    private CacheConfiguration buildCacheConfiguration(String name) {
        if (name.equals(CACHE_PERSON)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_PERSON);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity();

            entity.setKeyType(Integer.class.getName());

            entity.setValueType(Person.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("name", String.class.getName());
            fields.put("age", Integer.class.getName());

            entity.setFields(fields);

            ccfg.setQueryEntities(Arrays.asList(entity));

            return ccfg;
        }

        if (name.equals(CACHE_CALL)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_CALL);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(CallKey.class.getName(), Call.class.getName());

            Set<String> keyFields = new HashSet<>();

            keyFields.add("personId");
            keyFields.add("id");

            entity.setKeyFields(keyFields);

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("personId", Integer.class.getName());
            fields.put("id", Integer.class.getName());
            fields.put("name", String.class.getName());
            fields.put("duration", Integer.class.getName());

            entity.setFields(fields);

            ccfg.setQueryEntities(Arrays.asList(entity));

            return ccfg;
        }
        return null;
    }

    /** */
    @Test
    public void testUnicastQuerySelectAffinityKeyEqualsConstant() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select id, name, duration from Call where personId=100 order by id"), 1);

        assertEquals(2, result.size());

        checkResultsRow(result, 0, 1, "caller1", 100);
        checkResultsRow(result, 1, 2, "caller2", 200);
    }

    /** */
    @Test
    public void testUnicastQuerySelectAffinityKeyEqualsParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select id, name, duration from Call where personId=? order by id")
            .setArgs(100), 1);

        assertEquals(2, result.size());

        checkResultsRow(result, 0, 1, "caller1", 100);
        checkResultsRow(result, 1, 2, "caller2", 200);
    }

    /** */
    @Test
    public void testUnicastQuerySelectKeyEqualsParameterReused() throws Exception {
        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        for (int key : new int[] {0, 250, 500, 750, 1000} ) {
            List<List<?>> result = runQueryEnsureUnicast(cache,
                new SqlFieldsQuery("select name, age from Person where _key=?").setArgs(key), 1);

            assertEquals(1, result.size());

            Person person = cache.get(key);

            checkResultsRow(result, 0, person.name, person.age);
        }
    }

    /** */
    @Test
    public void testUnicastQuerySelectKeyEqualsParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        CallKey callKey = new CallKey(5, 1);

        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select name, duration from Call where _key=?")
            .setArgs(callKey), 1);

        assertEquals(1, result.size());

        Call call = cache.get(callKey);

        checkResultsRow(result, 0, call.name, call.duration);
    }

    /** Check group, having, ordering allowed to be unicast requests. */
    @Test
    public void testUnicastQueryGroups() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        String qry = "select name, count(1) " +
                "from Call " +
                "where personId = ? " +
                "group by name " +
                "having count(1) = 1 " +
                "order by name";

        final int personId = 10;

        List<List<?>> result = runQueryEnsureUnicast(cache, new SqlFieldsQuery(qry).setArgs(personId), 1);

        assertEquals(2, result.size());

        checkResultsRow(result, 0, "caller1", 1L);
        checkResultsRow(result, 1, "caller2", 1L);
    }

    /** */
    @Test
    public void testUnicastQuerySelectKeyEqualAndFieldParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        CallKey callKey = new CallKey(5, 1);

        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select name, duration from Call where _key=? and duration=?")
            .setArgs(callKey, 100), 1);

        assertEquals(1, result.size());

        Call call = cache.get(callKey);

        checkResultsRow(result, 0, call.name, call.duration);
    }

    /** */
    @Test
    public void testUnicastQuerySelect2KeyEqualsAndFieldParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        CallKey callKey1 = new CallKey(5, 1);
        CallKey callKey2 = new CallKey(1000, 1);

        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select name, duration from Call where (_key=? and duration=?) or (_key=?)")
            .setArgs(callKey1, 100, callKey2), 2);

        assertEquals(2, result.size());

        Call call = cache.get(callKey1);

        checkResultsRow(result, 0, call.name, call.duration);

        call = cache.get(callKey2);

        checkResultsRow(result, 1, call.name, call.duration);
    }

    /** */
    @Test
    public void testUnicastQueryKeyTypeConversionParameter() throws Exception {
        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        // Pass string argument to expression with integer
        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select name, age from Person where _key = ?")
            .setArgs("5"), 1);

        Person person = cache.get(5);

        assertEquals(1, result.size());

        assertEquals(person.name, result.get(0).get(0));
        assertEquals(person.age, result.get(0).get(1));
    }

    /** */
    @Test
    public void testUnicastQueryKeyTypeConversionConstant() throws Exception {
        IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        // Use string within expression against integer key
        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select name, age from Person where _key = '5'"), 1);

        Person person = cache.get(5);

        assertEquals(1, result.size());

        assertEquals(person.name, result.get(0).get(0));
        assertEquals(person.age, result.get(0).get(1));
    }

    /** */
    @Test
    public void testUnicastQueryAffinityKeyTypeConversionParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        // Pass string argument to expression with integer
        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select id, name, duration from Call where personId=? order by id")
                .setArgs("100"), 1);

        assertEquals(2, result.size());

        checkResultsRow(result, 0, 1, "caller1", 100);
        checkResultsRow(result, 1, 2, "caller2", 200);
    }

    /** */
    @Test
    public void testUnicastQueryAffinityKeyTypeConversionConstant() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        // Use string within expression against integer key
        List<List<?>> result = runQueryEnsureUnicast(cache,
            new SqlFieldsQuery("select id, name, duration from Call where personId='100' order by id"), 1);

        assertEquals(2, result.size());

        checkResultsRow(result, 0, 1, "caller1", 100);
        checkResultsRow(result, 1, 2, "caller2", 200);
    }

    /** */
    @Test
    public void testBroadcastQuerySelectKeyEqualsOrFieldParameter() throws Exception {
        IgniteCache<CallKey, Call> cache = grid(NODE_CLIENT).cache(CACHE_CALL);

        CallKey callKey = new CallKey(5, 1);

        List<List<?>> result = runQueryEnsureBroadcast(cache,
            new SqlFieldsQuery("select name, duration from Call where _key=? or duration=?")
            .setArgs(callKey, 100));

        assertEquals(cache.size() / 2, result.size());
    }

    /** */
    @Test
    public void testUuidKeyAsByteArrayParameter() throws Exception {
        String cacheName = "uuidCache";

        CacheConfiguration<UUID, UUID> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ccfg.setIndexedTypes(UUID.class, UUID.class);

        IgniteCache<UUID, UUID> cache = grid(NODE_CLIENT).createCache(ccfg);

        try {
            int count = 10;

            UUID values[] = new UUID[count];

            for (int i = 0; i < count; i++) {
                UUID val = UUID.randomUUID();

                cache.put(val, val);

                values[i] = val;
            }

            for (UUID val : values) {
                byte[] arr = convertUuidToByteArray(val);

                List<List<?>> result = cache.query(new SqlFieldsQuery(
                    "select _val from UUID where _key = ?").setArgs(arr)).getAll();

                assertEquals(1, result.size());
                assertEquals(val, result.get(0).get(0));
            }
        }
        finally {
            cache.destroy();
        }
    }

    /** */
    @Test
    public void testDateKeyAsTimestampParameter() throws Exception {
        String cacheName = "dateCache";

        CacheConfiguration<Date, Date> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ccfg.setIndexedTypes(Date.class, Date.class);

        IgniteCache<Date, Date> cache = grid(NODE_CLIENT).createCache(ccfg);

        try {
            int count = 30;

            Date values[] = new Date[count];

            DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

            for (int i = 0; i < count; i++) {
                Date val = dateFormat.parse(String.format("%02d/06/2017", i + 1));

                cache.put(val, val);

                values[i] = val;
            }

            for (Date val : values) {
                Timestamp ts = new Timestamp(val.getTime());

                List<List<?>> result = cache.query(new SqlFieldsQuery(
                    "select _val from Date where _key = ?").setArgs(ts)).getAll();

                assertEquals(1, result.size());
                assertEquals(val, result.get(0).get(0));
            }
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * Convert UUID to byte[].
     *
     * @param val UUID to convert.
     * @return Result.
     */
    private byte[] convertUuidToByteArray(UUID val) {
        assert val != null;

        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);

        bb.putLong(val.getMostSignificantBits());

        bb.putLong(val.getLeastSignificantBits());

        return bb.array();
    }

    /** */
    private void fillCaches() {
        IgniteCache<CallKey, Call> callCache = grid(NODE_CLIENT).cache(CACHE_CALL);
        IgniteCache<Integer, Person> personCache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        int count = affinity(personCache).partitions();

        String[] names = {"John", "Bob", "James", "David", "Chuck"};

        for (int i = 0; i < count; i++) {
            Person person = new Person(names[i % names.length], 20 + (i % names.length));

            personCache.put(i, person);

            // each person gets 2 calls
            callCache.put(new CallKey(i, 1), new Call("caller1", 100));
            callCache.put(new CallKey(i, 2), new Call("caller2", 200));
        }
    }

    /** */
    private void checkResultsRow(List<List<?>> results, int rowId, Object... expected) throws Exception {
        assertTrue(rowId < results.size());

        List<?> row = results.get(rowId);

        assertEquals(expected.length, row.size());

        for (int col = 0; col < expected.length; ++col)
            assertEquals(expected[col], row.get(col));
    }

    /** Run query and check that only one node did generate 'query executed' event for it. */
    private List<List<?>> runQueryEnsureUnicast(IgniteCache<?,?> cache, SqlFieldsQuery qry, int nodeCnt) throws Exception {
        try (EventCounter evtCounter = new EventCounter(nodeCnt)) {
            List<List<?>> result = cache.query(qry).getAll();

            // do broadcast 'marker' query to ensure that we received all events from previous qry
            cache.query(new SqlFieldsQuery(
                MessageFormat.format(FINAL_QRY, cache.getName()))
                .setArgs(FINAL_QRY_PARAM)).getAll();

            // wait for all events from 'marker' query
            evtCounter.await();

            // return result set of first query
            return result;
        }
    }

    /** */
    private List<List<?>> runQueryEnsureBroadcast(IgniteCache<?, ?> cache, SqlFieldsQuery qry) throws Exception {
        final CountDownLatch execLatch = new CountDownLatch(NODE_COUNT);

        final IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CacheQueryExecutedEvent;

                CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                assertNotNull(qe.clause());

                execLatch.countDown();

                return true;
            }
        };

        for (int i = 0; i < NODE_COUNT; i++)
            grid(i).events().localListen(pred, EVT_CACHE_QUERY_EXECUTED);

        List<List<?>> result = cache.query(qry).getAll();

        assertTrue(execLatch.await(5000, MILLISECONDS));

        for (int i = 0; i < NODE_COUNT; i++)
            grid(i).events().stopLocalListen(pred);

        return result;
    }

    /** */
    private class EventCounter implements AutoCloseable {
        /** */
        final AtomicInteger cnt;

        /** */
        final CountDownLatch execLatch;

        /** */
        final IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CacheQueryExecutedEvent;

                CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                String cacheName = qe.cacheName();

                assert cacheName != null;

                if (!cacheName.equals(CACHE_PERSON) &&
                    !cacheName.equals(CACHE_CALL))
                    return true;

                assertNotNull(qe.clause());

                Object[] args = qe.arguments();

                if ((args != null) && (args.length > 0) && (args[0] instanceof String)) {
                    String strParam = (String)args[0];

                    if (FINAL_QRY_PARAM.equals(strParam)) {
                        execLatch.countDown();

                        return true;
                    }
                }
                cnt.decrementAndGet();

                return true;
            }
        };

        /** */
        private EventCounter(int cnt) {
            this.cnt = new AtomicInteger(cnt);

            this.execLatch = new CountDownLatch(NODE_COUNT);

            for (int i = 0; i < NODE_COUNT; i++)
                grid(i).events().localListen(pred, EVT_CACHE_QUERY_EXECUTED);
        }

        /** */
        public void await() throws Exception {
            assertTrue(execLatch.await(5000, MILLISECONDS));

            assertEquals(0, cnt.get());
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            for (int i = 0; i < NODE_COUNT; i++)
                grid(i).events().stopLocalListen(pred);
        }
    }

    /** */
    private static class Person {
        /** */
        private String name;

        /** */
        private int age;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /** */
        @Override public int hashCode() {
            return name.hashCode() ^ age;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Person))
                return false;

            Person other = (Person)o;

            return name.equals(other.name) && age == other.age;
        }
    }

    /** */
    private static class CallKey {
        /** */
        @AffinityKeyMapped
        private int personId;

        /** */
        private int id;

        /** */
        private CallKey(int personId, int id) {
            this.personId = personId;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return personId ^ id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof CallKey))
                return false;

            CallKey other = (CallKey)o;

            return this.personId == other.personId && this.id == other.id;
        }
    }

    /** */
    private static class Call {
        /** */
        private String name;

        /** */
        private int duration;

        /** */
        public Call(String name, int duration) {
            this.name = name;

            this.duration = duration;
        }

        /** */
        @Override public int hashCode() {
            return name.hashCode() ^ duration;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Call))
                return false;

            Call other = (Call)o;

            return name.equals(other.name) && duration == other.duration;
        }
    }
}

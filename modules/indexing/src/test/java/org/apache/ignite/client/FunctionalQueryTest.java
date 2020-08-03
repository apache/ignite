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

package org.apache.ignite.client;

import java.lang.invoke.SerializedLambda;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Thin client functional tests.
 */
public class FunctionalQueryTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#query(Query)}</li>
     * </ul>
     */
    @Test
    public void testQueries() throws Exception {
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        // No peer class loading from thin clients: we need the server to know about this class to deserialize
        // ScanQuery filter.
        srvCfg.setBinaryConfiguration(new BinaryConfiguration().setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(getClass().getName()),
            new BinaryTypeConfiguration(SerializedLambda.class.getName())
        )));

        try (Ignite ignored = Ignition.start(srvCfg);
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Integer, Person> cache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

            Map<Integer, Person> data = IntStream.rangeClosed(1, 100).boxed()
                .collect(Collectors.toMap(i -> i, i -> new Person(i, String.format("Person %s", i))));

            cache.putAll(data);

            int minId = data.size() / 2 + 1;
            int pageSize = (data.size() - minId) / 3;
            int expSize = data.size() - minId + 1; // expected query result size

            // Expected result
            Map<Integer, Person> exp = data.entrySet().stream()
                .filter(e -> e.getKey() >= minId)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Scan and SQL queries
            Collection<Query<Cache.Entry<Integer, Person>>> queries = Arrays.asList(
                new ScanQuery<Integer, Person>((i, p) -> p.getId() >= minId).setPageSize(pageSize),
                new SqlQuery<Integer, Person>(Person.class, "id >= ?").setArgs(minId).setPageSize(pageSize)
            );

            for (Query<Cache.Entry<Integer, Person>> qry : queries) {
                try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache.query(qry)) {
                    List<Cache.Entry<Integer, Person>> res = cur.getAll();

                    assertEquals(
                        String.format("Unexpected number of rows from %s", qry.getClass().getSimpleName()),
                        expSize, res.size()
                    );

                    Map<Integer, Person> act = res.stream()
                        .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

                    assertEquals(String.format("unexpected rows from %s", qry.getClass().getSimpleName()), exp, act);
                }
            }

            checkSqlFieldsQuery(cache, minId, pageSize, expSize, exp, true);
            checkSqlFieldsQuery(cache, minId, pageSize, expSize, exp, false);
        }
    }

    /**
     * @param cache Cache.
     * @param minId Minimal ID.
     * @param pageSize Page size.
     * @param expSize The size of the expected results.
     * @param exp Expected results.
     * @param lazy Lazy mode flag.
     */
    private void checkSqlFieldsQuery(ClientCache<Integer, Person> cache, int minId, int pageSize, int expSize,
        Map<Integer, Person> exp, boolean lazy) {
        SqlFieldsQuery qry = new SqlFieldsQuery("select id, name from Person where id >= ?")
            .setArgs(minId)
            .setPageSize(pageSize)
            .setLazy(lazy);

        try (QueryCursor<List<?>> cur = cache.query(qry)) {
            List<List<?>> res = cur.getAll();

            assertEquals(expSize, res.size());

            Map<Integer, Person> act = res.stream().collect(Collectors.toMap(
                r -> Integer.parseInt(r.get(0).toString()),
                r -> new Person(Integer.parseInt(r.get(0).toString()), r.get(1).toString())
            ));

            assertEquals(exp, act);
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link IgniteClient#query(SqlFieldsQuery)}</li>
     * </ul>
     */
    @Test
    public void testSql() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration()); Ignite ignored2 = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            client.query(
                new SqlFieldsQuery(String.format(
                    "CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s\"",
                    Person.class.getName()
                )).setSchema("PUBLIC")
            ).getAll();

            final int KEY_COUNT = 10;

            for (int i = 0; i < KEY_COUNT; ++i) {
                int key = i;
                Person val = new Person(key, "Person " + i);

                client.query(new SqlFieldsQuery(
                    "INSERT INTO Person(id, name) VALUES(?, ?)"
                ).setArgs(val.getId(), val.getName()).setSchema("PUBLIC"))
                    .getAll();
            }

            Object cachedName = client.query(
                new SqlFieldsQuery("SELECT name from Person WHERE id=?").setArgs(1).setSchema("PUBLIC")
            ).getAll().iterator().next().iterator().next();

            assertEquals("Person 1", cachedName);

            List<List<?>> rows = client.query(
                new SqlFieldsQuery("SELECT * from Person WHERE id >= ?")
                    .setSchema("PUBLIC")
                    .setArgs(0)
                    .setPageSize(1)
            ).getAll();

            assertEquals(KEY_COUNT, rows.size());
        }
    }

    /** */
    @Test
    public void testGettingEmptyResultWhenQueryingEmptyTable() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            final String TBL = "Person";

            client.query(
                new SqlFieldsQuery(String.format(
                    "CREATE TABLE IF NOT EXISTS " + TBL + " (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s\"",
                    Person.class.getName()
                )).setSchema("PUBLIC")
            ).getAll();

            // IgniteClient#query() API
            List<List<?>> res = client.query(new SqlFieldsQuery("SELECT * FROM " + TBL)).getAll();

            assertNotNull(res);
            assertEquals(0, res.size());

            // ClientCache#query(SqlFieldsQuery) API
            ClientCache<Integer, Person> cache = client.cache("SQL_PUBLIC_" + TBL.toUpperCase());

            res = cache.query(new SqlFieldsQuery("SELECT * FROM " + TBL)).getAll();

            assertNotNull(res);
            assertEquals(0, res.size());

            // ClientCache#query(ScanQuery) and ClientCache#query(SqlQuery) API
            Collection<Query<Cache.Entry<Integer, Person>>> queries = Arrays.asList(
                new ScanQuery<>(),
                new SqlQuery<>(Person.class, "1 = 1")
            );

            for (Query<Cache.Entry<Integer, Person>> qry : queries) {
                try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache.query(qry)) {
                    List<Cache.Entry<Integer, Person>> res2 = cur.getAll();

                    assertNotNull(res2);
                    assertEquals(0, res2.size());
                }
            }
        }
    }

    /** */
    @Test
    public void testMixedQueryAndCacheApiOperations() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(
                 new ClientConfiguration().setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true))
                     .setAddresses(Config.SERVER))
        ) {
            String cacheName = "PersonCache";

            client.query(
                new SqlFieldsQuery(String.format(
                    "CREATE TABLE IF NOT EXISTS Person (key INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s,CACHE_NAME=%s\"",
                    Person.class.getName(), cacheName
                )).setSchema("PUBLIC")
            ).getAll();

            client.query(new SqlFieldsQuery("INSERT INTO Person(key, name) VALUES(?, ?)")
                .setArgs(1, "Person 1")
                .setSchema("PUBLIC")
            ).getAll();

            ClientCache<Integer, Person> cache = client.cache(cacheName);

            cache.put(2, new Person(2, "Person 2"));

            assertEquals("Person 1", cache.get(1).getName());

            assertEquals("Person 2", client.query(
                new SqlFieldsQuery("SELECT name FROM PUBLIC.Person WHERE key = 2")).getAll().get(0).get(0));
        }
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration().setAddresses(Config.SERVER)
            .setSendBufferSize(0)
            .setReceiveBufferSize(0);
    }
}

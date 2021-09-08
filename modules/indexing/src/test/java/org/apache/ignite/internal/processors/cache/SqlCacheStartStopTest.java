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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Start / stop caches and checks availability metadata via SQL.
 */
public class SqlCacheStartStopTest extends GridCommonAbstractTest {
    /** Cache names. */
    public static final String[] CACHE_NAMES = new String[] {
        null,
        "SQL_C1",
        "SQL_C2",
        "SQL_C3",
        "SQL_C4",
        "SQL_C5",
        "SQL_C6",
        "SQL_C7",
        "SQL_C8",
        "SQL_C9",
        "SQL_C10",
        "SQL_C11",
        "SQL_C12"
    };

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     */
    @Test
    public void testClientCacheClose() throws Exception {
        int[] idxs = IntStream.rangeClosed(1, 9).toArray();

        IgniteConfiguration srvCfg = getConfiguration("server")
            .setCacheConfiguration(
                cacheConfig(1),
                cacheConfig(2)
            );

        IgniteEx srv = startGrid(srvCfg);

        createCache(srv, 4);

        createTable(srv, 5);

        IgniteConfiguration cliCfg = getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(
                cacheConfig(1),
                cacheConfig(3)
            );

        IgniteEx cli = startGrid(cliCfg);

        createCache(srv, 6);

        createTable(srv, 7);

        createCache(cli, 8);

        execSql(cache(cli, 1), createTableQuery(9));

        for (int i : idxs) {
            execSql(srv, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "srv", "srv");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
        }

        for (int i : idxs)
            cache(cli, i).close();

        IgniteCache<Object, Object> c10 = createCache(cli, 10);
        IgniteCache<Object, Object> c11 = cli.createCache(cacheConfigWithoutSql(11));

        for (int i : idxs) {
            String cacheName = CACHE_NAMES[i];

            execSql(cli, "select * from " + cacheName);
            execSql(cli, "create index idx_" + cacheName + " on " + cacheName + "(s)");
            execSql(cli, "select count(*) from " + cacheName);
            execSql(cli, "drop index idx_" + cacheName);

            execSql(c10, "select * from " + cacheName);
            execSql(c10, "create index idx_" + cacheName + " on " + cacheName + "(s)");
            execSql(c11, "select count(*) from " + cacheName);
            execSql(c11, "drop index idx_" + cacheName);
        }
    }

    /**
     */
    @Test
    public void testServerCacheClose() throws Exception {
        int[] idxs = IntStream.rangeClosed(1, 11).toArray();

        CacheConfiguration[] ccfgs = {cacheConfig(1), cacheConfig(2)};

        IgniteEx n1 = startGrid(getConfiguration("n1").setCacheConfiguration(ccfgs));
        IgniteEx n2 = startGrid(getConfiguration("n2").setCacheConfiguration(ccfgs));

        createCache(n1, 4);

        createTable(n1, 5);

        createCache(n2, 6);

        createTable(n2, 7);

        IgniteEx cli = startGrid(getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(cacheConfig(1), cacheConfig(3), cacheConfig(6))
        );

        createCache(n1, 8);

        createTable(n2, 9);

        createCache(cli, 10);

        execSql(cache(cli, 1), createTableQuery(11));

        for (int i : idxs) {
            execSql(n1, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "n1", "n1");
            execSql(n2, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "n2", "n2");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
        }

        for (int i : idxs)
            cache(n1, i).close();

        n1.createCache(cacheConfig(12));

        for (int i : idxs) {
            String cacheName = CACHE_NAMES[i];

            execSql(cache(cli, 1), "select * from " + cacheName);
            execSql(cache(cli, 3), "create index idx_" + cacheName + " on " + cacheName + "(s)");
            execSql(cache(n1, 12), "select count(*) from " + cacheName + " group by id");
            execSql(cache(n1, 12), "drop index idx_" + cacheName);
        }
    }

    /**
     */
    @Test
    public void testClientCacheDestroy() throws Exception {
        checkDestroy(true);
    }

    /**
     */
    @Test
    public void testServerCacheDestroy() throws Exception {
        checkDestroy(false);
    }

    /**
     */
    private void checkDestroy(boolean client) throws Exception {
        int[] idxs = IntStream.rangeClosed(1, 9).toArray();

        IgniteConfiguration srvCfg = getConfiguration("server")
            .setCacheConfiguration(
                cacheConfig(1),
                cacheConfig(2)
            );

        IgniteEx srv = startGrid(srvCfg);

        createCache(srv, 4);

        createTable(srv, 5);

        IgniteConfiguration cliCfg = getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(
                cacheConfig(1),
                cacheConfig(3)
            );

        IgniteEx cli = startGrid(cliCfg);

        createCache(srv, 6);

        createTable(srv, 7);

        createCache(cli, 8);

        execSql(cache(cli, 1), createTableQuery(9));

        for (int i : idxs) {
            execSql(srv, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "srv", "srv");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
        }

        for (int i : idxs) {
            Ignite node = client ? cli : srv;

            cache(node, i).destroy();
        }

        IgniteCache<Object, Object> c10 = createCache(cli, 10);

        for (int i : idxs) {
            String cacheName = CACHE_NAMES[i];

            assertThrowsWithCause(() -> execSql(c10, "select * from " + cacheName), SQLException.class);
        }
    }

    /**
     */
    private IgniteCache<Object, Object> createCache(IgniteEx ign, int i) {
        return ign.createCache(cacheConfig(i));
    }

    /**
     */
    private void createTable(IgniteEx ign, int i) {
        execSql(ign, createTableQuery(i));
    }

    /**
     */
    private String createTableQuery(int i) {
        String name = CACHE_NAMES[i];

        return "create table " + name + "(id varchar primary key, s varchar) with \"cache_name=" + name + "\"";
    }

    /**
     */
    private List<List<?>> execSql(IgniteCache<?, ?> cache, String sql, Object... args) {
        return cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /**
     */
    private List<List<?>> execSql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param i Index of the cache (see {@link #CACHE_NAMES}).
     */
    private CacheConfiguration<Object, Object> cacheConfig(int i) {
        String name = CACHE_NAMES[i];

        return new CacheConfiguration<>(name)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(new QueryEntity(Key.class, Val.class)
                .setTableName(name)));
    }

    /**
     * @param i Index of the cache (see {@link #CACHE_NAMES}).
     */
    private CacheConfiguration<Object, Object> cacheConfigWithoutSql(int i) {
        return new CacheConfiguration<>(CACHE_NAMES[i]);
    }

    /**
     * @param i Index of the cache (see {@link #CACHE_NAMES}).
     */
    private IgniteCache<Object, Object> cache(Ignite ign, int i) {
        return ign.cache(CACHE_NAMES[i]);
    }

    /**
     */
    public static class Key {
        /** */
        @QuerySqlField
        private final String id;

        /**
         */
        public Key(String id) {
            this.id = id;
        }
    }

    /**
     */
    public static class Val {
        /** */
        @QuerySqlField
        private final String s;

        /**
         */
        public Val(String s) {
            this.s = s;
        }
    }
}

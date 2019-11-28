/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 */
public class SqlCacheStartStopTest extends GridCommonAbstractTest {
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
    @Ignore("https://ggsystems.atlassian.net/browse/GG-26054")
    @Test
    public void testClientCloseQueryConcurrent() throws Exception {
        IgniteConfiguration srvCfg = getConfiguration("server")
            .setCacheConfiguration(
                ccfg(1)
            );

        IgniteEx srv = startGrid(srvCfg);

        IgniteEx cli = startGrid(getConfiguration("client")
                .setClientMode(true)
        );

        execSql(srv, "insert into " + CACHE_NAMES[1] + "(id, s) values(?, ?)", "cli", "cli");

        assertEquals(1, execSql(cli, "SELECT * FROM " + CACHE_NAMES[1]).size());
        execSql(cli, "create index idx_" + CACHE_NAMES[1] + " on " + CACHE_NAMES[1] + "(s)");
        execSql(cli, "drop index idx_" + CACHE_NAMES[1]);

        AtomicBoolean end = new AtomicBoolean();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            while (!end.get())
                cli.cache(CACHE_NAMES[1]).close();
        });

        for (int i = 0; i < 100000; ++i) {
            assertEquals(1, execSql(cli, "SELECT * FROM " + CACHE_NAMES[1]).size());
            execSql(cli, "create index idx_" + CACHE_NAMES[1] + " on " + CACHE_NAMES[1] + "(s)");
            execSql(cli, "drop index idx_" + CACHE_NAMES[1]);
        }

        end.set(true);

        fut.get(1000);
    }

    /**
     */
    @Test
    public void testClientCacheClose() throws Exception {
        int[] idxs = IntStream.rangeClosed(1, 9).toArray();

        IgniteConfiguration srvCfg = getConfiguration("server")
            .setCacheConfiguration(
                ccfg(1),
                ccfg(2)
            );

        IgniteEx srv = startGrid(srvCfg);

        createCache(srv, 4);

        createTable(srv, 5);

        IgniteConfiguration cliCfg = getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(
                ccfg(1),
                ccfg(3)
            );

        IgniteEx cli = startGrid(cliCfg);

        createCache(srv, 6);

        createTable(srv, 7);

        createCache(cli, 8);

        execSql(cache(cli, 1), createTableQuery(9));

        for (int i : idxs) {
            execSql(srv, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "srv", "srv");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
            // assert operation does not fail
        }

        for (int i : idxs)
            cache(cli, i).close();

        IgniteCache<Object, Object> c10 = createCache(cli, 10);
        IgniteCache<Object, Object> c11 = cli.createCache(ccfgWithoutSql(11));

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
            // assert operation does not fail
        }
    }

    /**
     */
    @Test
    public void testServerCacheClose() throws Exception {
        int[] idxs = IntStream.rangeClosed(1, 11).toArray();

        CacheConfiguration[] ccfgs = {ccfg(1), ccfg(2)};

        IgniteEx n1 = startGrid(getConfiguration("n1").setCacheConfiguration(ccfgs));
        IgniteEx n2 = startGrid(getConfiguration("n2").setCacheConfiguration(ccfgs));

        createCache(n1, 4);

        createTable(n1, 5);

        createCache(n2, 6);

        createTable(n2, 7);

        IgniteEx cli = startGrid(getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(ccfg(1), ccfg(3), ccfg(6))
        );

        createCache(n1, 8);

        createTable(n2, 9);

        createCache(cli, 10);

        execSql(cache(cli, 1), createTableQuery(11));

        for (int i : idxs) {
            execSql(n1, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "n1", "n1");
            execSql(n2, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "n2", "n2");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
            // assert operation does not fail
        }

        for (int i : idxs)
            cache(n1, i).close();

        n1.createCache(ccfg(12));

        for (int i : idxs) {
            String cacheName = CACHE_NAMES[i];

            execSql(cache(cli, 1), "select * from " + cacheName);
            execSql(cache(cli, 3), "create index idx_" + cacheName + " on " + cacheName + "(s)");
            execSql(cache(n1, 12), "select count(*) from " + cacheName + " group by id");
            execSql(cache(n1, 12), "drop index idx_" + cacheName);
            // assert operation does not fail
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
                ccfg(1),
                ccfg(2)
            );

        IgniteEx srv = startGrid(srvCfg);

        createCache(srv, 4);

        createTable(srv, 5);

        IgniteConfiguration cliCfg = getConfiguration("client")
            .setClientMode(true)
            .setCacheConfiguration(
                ccfg(1),
                ccfg(3)
            );

        IgniteEx cli = startGrid(cliCfg);

        createCache(srv, 6);

        createTable(srv, 7);

        createCache(cli, 8);

        execSql(cache(cli, 1), createTableQuery(9));

        for (int i : idxs) {
            execSql(srv, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "srv", "srv");
            execSql(cli, "insert into " + CACHE_NAMES[i] + "(id, s) values(?, ?)", "cli", "cli");
            // assert operation does not fail
        }

        for (int i : idxs) {
            Ignite node = client ? cli : srv;

            cache(node, i).destroy();
        }

        IgniteCache<Object, Object> c10 = createCache(cli, 10);

        for (int i : idxs) {
            String cacheName = CACHE_NAMES[i];

            assertThrowsWithCause(() -> execSql(c10, "select * from " + cacheName), SQLException.class);
            // assert operation does not fail
        }
    }

    /**
     */
    private IgniteCache<Object, Object> createCache(IgniteEx ign, int i) {
        return ign.createCache(ccfg(i));
    }

    /**
     */
    private void createTable(IgniteEx ign, int i) {
        String name = CACHE_NAMES[i];

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
     */
    private CacheConfiguration<Object, Object> ccfg(int i) {
        String name = CACHE_NAMES[i];

        return new CacheConfiguration<>(name)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(new QueryEntity(Key.class, Val.class)
                .setTableName(name)));
    }

    /**
     */
    private CacheConfiguration<Object, Object> ccfgWithoutSql(int i) {
        return new CacheConfiguration<>(CACHE_NAMES[i]);
    }

    /**
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

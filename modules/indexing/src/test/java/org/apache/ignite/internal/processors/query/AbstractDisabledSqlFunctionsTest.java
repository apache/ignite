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

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for disabled SQL functions.
 */
public abstract class AbstractDisabledSqlFunctionsTest extends GridCommonAbstractTest {
    /** Pattern func not found. */
    private static final Pattern PTRN_FUNC_NOT_FOUND = Pattern.compile("Failed to parse query. Function \"\\w+\" not found");

    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        FileSystem fs = FileSystems.getDefault();

        Files.deleteIfExists(fs.getPath("test.dat"));
        Files.deleteIfExists(fs.getPath("test.csv"));
        Files.deleteIfExists(fs.getPath("test.mv.db"));

        super.afterTest();
    }

    /**
     * Setup before test.
     * Used instead of #beforeTest because there are tests with custom system properties.
     * @throws Exception On error.
     */
    private void init() throws Exception {
        startGrid("srv");
        startGrid("cli");

        IgniteCache<Long, Long> c = grid("srv").createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);

    }

    /**
     */
    public void testDefaultSelect() throws Exception {
        init();

        checkSqlWithDisabledFunction("SELECT FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("SELECT FILE_READ('test.dat')");
        checkSqlWithDisabledFunction("SELECT CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("SELECT * FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("SELECT MEMORY_FREE()");
        checkSqlWithDisabledFunction("SELECT MEMORY_USED()");
        checkSqlWithDisabledFunction("SELECT LOCK_MODE()");
        checkSqlWithDisabledFunction("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("SELECT SESSION_ID()");
        checkSqlWithDisabledFunction("SELECT CANCEL_SESSION(1)");
    }

    /**
     */
    public void testDefaultInsert() throws Exception {
        init();

        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, FILE_READ('test.dat')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, SELECT CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, MEMORY_FREE()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, MEMORY_USED()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, LOCK_MODE()");
        checkSqlWithDisabledFunction(
            "INSERT INTO TEST (ID, VAL) SELECT 1, LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, SESSION_ID()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, CANCEL_SESSION(1)");
    }

    /**
     */
    public void testDefaultUpdate() throws Exception {
        init();

        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LENGTH(FILE_READ('test.dat'))");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = SELECT count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = MEMORY_FREE()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = MEMORY_USED()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LOCK_MODE()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = SESSION_ID()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = CANCEL_SESSION(1)");
    }

    /**
     */
    public void testDefaultDelete() throws Exception {
        init();

        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LENGTH(FILE_READ('test.dat'))");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = SELECT count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = MEMORY_FREE()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = MEMORY_USED()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LOCK_MODE()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = SESSION_ID()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = CANCEL_SESSION(1)");
    }

    /**
     */
    public void testAllowFunctionsDisabledByDefault() throws Exception {
        init();

        setDisabledSqlFunction();

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        sql("SELECT FILE_READ('test.dat')").getAll();
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();
    }

    /**
     */
    public void testAllowFunctionsDisabledByDefaultWithSysProperty() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS, "");

        try {
            init();

            sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
            sql("SELECT FILE_READ('test.dat')").getAll();
            sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
            sql("SELECT * FROM CSVREAD('test.csv')").getAll();
            sql("SELECT MEMORY_FREE()").getAll();
            sql("SELECT MEMORY_USED()").getAll();
            sql("SELECT LOCK_MODE()").getAll();
            sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
            sql("SELECT SESSION_ID()").getAll();
            sql("SELECT CANCEL_SESSION(1)").getAll();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS);

        }
    }

    /**
     */
    public void testCustomDisabledFunctionsSet_Length() throws Exception {
        init();

        setDisabledSqlFunction("LENGTH");

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        sql("SELECT FILE_READ('test.dat')").getAll();
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();

        checkSqlWithDisabledFunction("SELECT LENGTH(?)", "test");
    }

    /**
     */
    public void testCustomDisabledFunctionsSetBySysProperty_Length() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS, "LENGTH");

        try {
            init();

            sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
            sql("SELECT FILE_READ('test.dat')").getAll();
            sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
            sql("SELECT * FROM CSVREAD('test.csv')").getAll();
            sql("SELECT MEMORY_FREE()").getAll();
            sql("SELECT MEMORY_USED()").getAll();
            sql("SELECT LOCK_MODE()").getAll();
            sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
            sql("SELECT SESSION_ID()").getAll();
            sql("SELECT CANCEL_SESSION(1)").getAll();

            checkSqlWithDisabledFunction("SELECT LENGTH(?)", "test");
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS);
        }
    }

    /**
     */
    public void testCustomDisabledFunctionsSet_FileRead_User() throws Exception {
        init();

        setDisabledSqlFunction("FILE_READ", "USER");

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        checkSqlWithDisabledFunction("SELECT FILE_READ('test.dat')");
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();

        checkSqlWithDisabledFunction("SELECT USER()");

        sql("SELECT CURRENT_TIMESTAMP()").getAll();
    }

    /**
     */
    public void testCustomDisabledFunctionsSetBySysProperty_FileRead_User() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS, "FILE_READ, USER");

        try {
            init();

            sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
            checkSqlWithDisabledFunction("SELECT FILE_READ('test.dat')");
            sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
            sql("SELECT * FROM CSVREAD('test.csv')").getAll();
            sql("SELECT MEMORY_FREE()").getAll();
            sql("SELECT MEMORY_USED()").getAll();
            sql("SELECT LOCK_MODE()").getAll();
            sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
            sql("SELECT SESSION_ID()").getAll();
            sql("SELECT CANCEL_SESSION(1)").getAll();

            checkSqlWithDisabledFunction("SELECT USER()");

            sql("SELECT CURRENT_TIMESTAMP()").getAll();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_SQL_DISABLED_FUNCTIONS);
        }
    }

    /**
     */
    private void checkSqlWithDisabledFunction(final String sql, final Object ... args) {
        try {
            sql(sql, args).getAll();

            fail("Exception must be thrown");
        }
        catch (IgniteSQLException e) {
            Matcher m = PTRN_FUNC_NOT_FOUND.matcher(e.getMessage());

            assertTrue("Unexpected error message: " + e.getMessage(), m.find());
        }
        catch (Throwable e) {
            log.error("Unexpected exception", e);

            fail("Unexpected exception");
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        IgniteEx ign = executeOnClientNode() ? grid("cli") :grid("srv");

        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(localQuery())
            .setArgs(args), false);
    }

    /**
     * @return {@code true} if the query is executed on client node.
     */
    protected abstract boolean executeOnClientNode();

    /**
     * @return {@code true} if the query is executed locally.
     */
    protected abstract boolean localQuery();

    /**
     * @param funcs Disabled SQL functions.
     * @throws IgniteCheckedException On error.
     */
    private void setDisabledSqlFunction(String... funcs) throws IgniteCheckedException {
        HashSet<String> set = new HashSet<>(Arrays.stream(funcs).collect(Collectors.toSet()));

        for (Ignite g : G.allGrids()) {
            ((IgniteH2Indexing)((IgniteEx)g).context().query().getIndexing())
                .functionsManager()
                .updateDisabledFunctions(set);
        }
    }
}

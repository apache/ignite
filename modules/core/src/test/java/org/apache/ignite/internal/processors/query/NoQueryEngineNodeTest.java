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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl.NO_QRY_ENGINE_MSG;
import static org.apache.ignite.internal.processors.query.GridQueryProcessor.INDEXING_DISABLED_MSG;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
@SuppressWarnings("deprecation")
public class NoQueryEngineNodeTest extends GridCommonAbstractTest {
    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1";

    /** */
    public static final String H2_ONLY_QRY_MSG = "is supported by the H2 query engine only. Add module " +
        "'ignite-indexing' to the classpath of all Ignite nodes";

    /** */
    private static IgniteCache<Integer, String> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cache = startGrid(0).cache(DEFAULT_CACHE_NAME);

        assertFalse("Test must run on a node without any query engine", grid(0).context().query().moduleEnabled());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** */
    @Test
    public void testSqlFieldsQuery() {
        assertThrows(log, () -> cache.query(new SqlFieldsQuery("SELECT 1")).getAll(), CacheException.class, NO_QRY_ENGINE_MSG);
    }

    /** */
    @Test
    public void testSqlQuery() {
        Throwable e = assertThrows(
            log,
            () -> cache.query(new SqlQuery<Integer, String>(String.class, "1 = 1")).getAll(),
            CacheException.class,
            "'SqlQuery' " + H2_ONLY_QRY_MSG + "."
        );

        assertFalse(e.getMessage(), e.getMessage().contains(SqlFieldsQuery.class.getSimpleName()));
    }

    /** */
    @Test
    public void testTextQuery() {
        assertThrows(
            log,
            () -> cache.query(new TextQuery<Integer, String>(String.class, "text")).getAll(),
            CacheException.class,
            "'TextQuery' " + H2_ONLY_QRY_MSG + "."
        );
    }

    /** */
    @Test
    public void testJdbc() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            assertThrowsAnyCause(log, () -> stmt.executeQuery("SELECT 1"), SQLException.class, INDEXING_DISABLED_MSG);
        }
    }

    /** */
    @Test
    public void testJdbcQueryEngineProperty() {
        assertThrowsAnyCause(
            log,
            () -> DriverManager.getConnection(JDBC_URL + "?queryEngine=calcite"),
            SQLException.class,
            "Not found configuration for query engine: calcite"
        );
    }
}

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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for empty cache.
 */
@RunWith(JUnit4.class)
public class JdbcThinEmptyCacheSelfTest extends JdbcThinAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(CACHE_NAME);
        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            Byte.class, Byte.class
        );

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + CACHE_NAME + '"');

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.getConnection().close();

            stmt.close();

            assert stmt.isClosed();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectNumber() throws Exception {
        ResultSet rs = stmt.executeQuery("select 1");

        int cnt = 0;

        while (rs.next()) {
            assert rs.getInt(1) == 1;
            assert "1".equals(rs.getString(1));

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectString() throws Exception {
        ResultSet rs = stmt.executeQuery("select 'str'");

        int cnt = 0;

        while (rs.next()) {
            assertEquals("str", rs.getString(1));

            cnt++;
        }

        assert cnt == 1;
    }
}

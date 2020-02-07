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
 *
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test expose SPATIAL indexes through SQL system view INDEXES.
 */
public class H2IndexesSystemViewTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return super.getConfiguration().setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME));
    }

    /**
     * Test indexes system view.
     *
     * @throws Exception in case of failure.
     */
    @Test
    public void testIndexesView() throws Exception {
        IgniteEx srv = startGrid(getConfiguration());

        IgniteEx client = startClientGrid(getConfiguration().setIgniteInstanceName("CLIENT"));

        execSql("CREATE TABLE PUBLIC.AFF_CACHE (ID1 INT, ID2 INT, GEOM GEOMETRY, PRIMARY KEY (ID1))");

        execSql("CREATE SPATIAL INDEX IDX_GEO_1 ON PUBLIC.AFF_CACHE(GEOM)");

        String idxSql = "SELECT " +
            "  CACHE_ID," +
            "  CACHE_NAME," +
            "  SCHEMA_NAME," +
            "  TABLE_NAME," +
            "  INDEX_NAME," +
            "  INDEX_TYPE," +
            "  COLUMNS," +
            "  IS_PK," +
            "  IS_UNIQUE," +
            "  INLINE_SIZE" +
            " FROM SYS.INDEXES ORDER BY TABLE_NAME, INDEX_NAME";

        List<List<?>> srvNodeIndexes = execSql(srv, idxSql);

        List<List<?>> clientNodeNodeIndexes = execSql(client, idxSql);

        for (List<?> idx : clientNodeNodeIndexes)
            assertTrue(srvNodeIndexes.contains(idx));

        String[][] expectedResults = {
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "IDX_GEO_1", "SPATIAL", "\"GEOM\" ASC", "false", "false", "0"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC", "true", "true", "5"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC", "true", "true", "0"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK_proxy", "BTREE", "\"ID1\" ASC", "false", "false", "0"}
        };

        for (int i = 0; i < srvNodeIndexes.size(); i++) {
            List<?> resRow = srvNodeIndexes.get(i);

            String[] expRow = expectedResults[i];

            assertEquals(expRow.length, resRow.size());

            for (int j = 0; j < expRow.length; j++)
                assertEquals(expRow[j], String.valueOf(resRow.get(j)));
        }
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

}

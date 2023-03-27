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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/** Verifies custom sql schema within different configurations. */
public class IgniteSqlSchemasDiffConfigurationsTest extends AbstractIndexingCommonTest {
    /** */
    private static final String SCHEMA_NAME_1 = "SCHEMA_1";

    /** */
    private static final String SCHEMA_NAME_2 = "SCHEMA_2";

    /** */
    private static final String SCHEMA_NAME_3 = "SCHEMA_3";

    /** */
    private static final String SCHEMA_NAME_4 = "SCHEMA_4";

    /** */
    private static String t(String schema, String tbl) {
        return schema + "." + tbl;
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testDiffSqlSchemasCfgProp() throws Exception {
        startGrid(getConfiguration("ign1")
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas(SCHEMA_NAME_1, SCHEMA_NAME_2))
        );

        startGrid(getConfiguration("ign2")
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas(SCHEMA_NAME_3, SCHEMA_NAME_4))
        );

        List<List<String>> exp = Arrays.asList(
            Arrays.asList(SCHEMA_NAME_1, "cache_1"),
            Arrays.asList(SCHEMA_NAME_2, "cache_2"),
            Arrays.asList(SCHEMA_NAME_3, "cache_3"),
            Arrays.asList(SCHEMA_NAME_4, "cache_4")
        );

        for (List<String> row : exp)
            grid("ign1").createCache(new CacheConfiguration<>(row.get(1)).setSqlSchema(row.get(0)));

        List<List<?>> res = execSql("ign1", "SELECT SQL_SCHEMA, CACHE_NAME FROM "
            + t(QueryUtils.SCHEMA_SYS, "CACHES") + " WHERE CACHE_NAME LIKE 'cache%' ORDER BY SQL_SCHEMA");

        Assert.assertEquals(exp, res);
    }

    /** */
    private IgniteConfiguration createTestConf(String nodeName, String schemaName, String cacheName) throws Exception {
        return getConfiguration(nodeName)
            .setCacheConfiguration(new CacheConfiguration(cacheName).setSqlSchema(schemaName));
    }

    /** */
    protected List<List<?>> execSql(String ignName, String qry) {
        return grid(ignName).context().query()
            .querySqlFields(new SqlFieldsQuery(qry).setLazy(true), false)
            .getAll();
    }
}

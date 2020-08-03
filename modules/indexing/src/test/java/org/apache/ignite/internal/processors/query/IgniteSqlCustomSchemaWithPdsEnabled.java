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

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/** Test verifies custom sql schema behavior when PDS is enabled. */
public class IgniteSqlCustomSchemaWithPdsEnabled extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE_NAME = "cache_4";

    /** */
    private static final String SCHEMA_NAME_1 = "SCHEMA_1";

    /** */
    private static final String SCHEMA_NAME_2 = "SCHEMA_2";

    /** */
    private static final String SCHEMA_NAME_3 = "ScHeMa3";

    /** */
    private static final String SCHEMA_NAME_4 = "SCHEMA_4";

    /** */
    private static final String TABLE_NAME = "T1";

    /** */
    private static final String[] ALL_SCHEMAS =
        new String[]{SCHEMA_NAME_1, SCHEMA_NAME_2, SCHEMA_NAME_3, SCHEMA_NAME_4};

    /** */
    private static String t(String schema, String tbl) {
        return schema + "." + tbl;
    }

    /** */
    private static String q(String str) {
        return "\"" + str + "\"";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas(SCHEMA_NAME_1, SCHEMA_NAME_2, q(SCHEMA_NAME_3))
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSimpleRestart() throws Exception {
        startAndActivate().createCache(new CacheConfiguration<>(CACHE_NAME).setSqlSchema(SCHEMA_NAME_4));

        for (String schemaName : ALL_SCHEMAS) {
            execSql("CREATE TABLE " + t(q(schemaName), TABLE_NAME) + "(id INT PRIMARY KEY, val VARCHAR)");

            for (int i = 0; i < 10; i++)
                execSql("INSERT INTO " + t(q(schemaName), TABLE_NAME) + "(id, val) VALUES (" + i + ", '" + schemaName + "')");
        }

        stopGrid(0);

        startGrid(0).cluster().active(true);

        for (String schemaName : ALL_SCHEMAS) {
            List<List<?>> act = execSql("SELECT COUNT(*) FROM " + t(q(schemaName), TABLE_NAME)
                + " WHERE val = '" + schemaName + "'");

            Assert.assertEquals(10L, act.get(0).get(0));
        }
    }

    /** */
    @Test
    public void testRecreateAfterRestart() throws Exception {
        IgniteCache<?, ?> cache = startAndActivate().createCache(
            new CacheConfiguration<>(CACHE_NAME).setSqlSchema(SCHEMA_NAME_4));

        verifyCacheInSchema(CACHE_NAME, SCHEMA_NAME_4);

        cache.destroy();

        stopGrid(0);

        startAndActivate().createCache(new CacheConfiguration<>(CACHE_NAME).setSqlSchema(SCHEMA_NAME_4));

        verifyCacheInSchema(CACHE_NAME, SCHEMA_NAME_4);
    }

    /** */
    private void verifyCacheInSchema(String cacheName, String expSchema) {
        Object actSchema = execSql("SELECT SQL_SCHEMA FROM " + t(QueryUtils.SCHEMA_SYS, "CACHES")
            + " WHERE CACHE_NAME = '" + cacheName + "'").get(0).get(0);

        Assert.assertEquals(expSchema, actSchema);
    }

    /** */
    private IgniteEx startAndActivate() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        return ignite;
    }

    /** */
    private List<List<?>> execSql(String qry) {
        return grid(0).context().query()
            .querySqlFields(new SqlFieldsQuery(qry).setLazy(true), false)
            .getAll();
    }
}

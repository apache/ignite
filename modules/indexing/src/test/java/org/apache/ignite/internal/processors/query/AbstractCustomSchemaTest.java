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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/** Abstract test to verify custom sql schema. */
public abstract class AbstractCustomSchemaTest extends AbstractIndexingCommonTest {
    /** */
    protected static final String TBL_NAME = "T1";

    /** */
    protected static final String SCHEMA_NAME_1 = "SCHEMA_1";

    /** */
    protected static final String SCHEMA_NAME_2 = "SCHEMA_2";

    /** */
    private static final String SCHEMA_NAME_3 = "ScHeMa3";

    /** */
    private static final String SCHEMA_NAME_4 = "SCHEMA_4";

    /** */
    private static final String UNKNOWN_SCHEMA_NAME = "UNKNOWN_SCHEMA";

    /** */
    private static final String CACHE_NAME = "cache_4";

    /** */
    protected static String t(String schema, String tbl) {
        return schema + "." + tbl;
    }

    /** */
    private static String q(String str) {
        return "\"" + str + "\"";
    }

    /** */
    protected abstract List<List<?>> execSql(String qry);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas(SCHEMA_NAME_1, SCHEMA_NAME_2, q(SCHEMA_NAME_3))
            )
            .setCacheConfiguration(new CacheConfiguration(CACHE_NAME).setSqlSchema(SCHEMA_NAME_4));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** */
    @After
    public void clear() {
        execSql("DROP TABLE IF EXISTS " + t(SCHEMA_NAME_1, TBL_NAME));
        execSql("DROP TABLE IF EXISTS " + t(SCHEMA_NAME_2, TBL_NAME));
        execSql("DROP TABLE IF EXISTS " + t(q(SCHEMA_NAME_3), TBL_NAME));
        execSql("DROP TABLE IF EXISTS " + t(SCHEMA_NAME_4, TBL_NAME));
    }

    /** */
    @Test
    public void testBasicOpsDiffSchemas() {
        execSql("CREATE TABLE " + t(SCHEMA_NAME_1, TBL_NAME) + " (s1_key INT PRIMARY KEY, s1_val INT)");
        execSql("CREATE TABLE " + t(SCHEMA_NAME_2, TBL_NAME) + " (s2_key INT PRIMARY KEY, s2_val INT)");
        execSql("CREATE TABLE " + t(q(SCHEMA_NAME_3), TBL_NAME) + " (s3_key INT PRIMARY KEY, s3_val INT)");
        execSql("CREATE TABLE " + t(SCHEMA_NAME_4, TBL_NAME) + " (s4_key INT PRIMARY KEY, s4_val INT)");

        execSql("INSERT INTO " + t(SCHEMA_NAME_1, TBL_NAME) + " (s1_key, s1_val) VALUES (1, 2)");
        execSql("INSERT INTO " + t(SCHEMA_NAME_2, TBL_NAME) + " (s2_key, s2_val) VALUES (1, 2)");
        execSql("INSERT INTO " + t(q(SCHEMA_NAME_3), TBL_NAME) + " (s3_key, s3_val) VALUES (1, 2)");
        execSql("INSERT INTO " + t(SCHEMA_NAME_4, TBL_NAME) + " (s4_key, s4_val) VALUES (1, 2)");

        execSql("UPDATE " + t(SCHEMA_NAME_1, TBL_NAME) + " SET s1_val = 5");
        execSql("UPDATE " + t(SCHEMA_NAME_2, TBL_NAME) + " SET s2_val = 5");
        execSql("UPDATE " + t(q(SCHEMA_NAME_3), TBL_NAME) + " SET s3_val = 5");
        execSql("UPDATE " + t(SCHEMA_NAME_4, TBL_NAME) + " SET s4_val = 5");

        execSql("DELETE FROM " + t(SCHEMA_NAME_1, TBL_NAME));
        execSql("DELETE FROM " + t(SCHEMA_NAME_2, TBL_NAME));
        execSql("DELETE FROM " + t(q(SCHEMA_NAME_3), TBL_NAME));
        execSql("DELETE FROM " + t(SCHEMA_NAME_4, TBL_NAME));

        execSql("CREATE INDEX t1_idx_1 ON " + t(SCHEMA_NAME_1, TBL_NAME) + "(s1_val)");
        execSql("CREATE INDEX t1_idx_1 ON " + t(SCHEMA_NAME_2, TBL_NAME) + "(s2_val)");
        execSql("CREATE INDEX t1_idx_1 ON " + t(q(SCHEMA_NAME_3), TBL_NAME) + "(s3_val)");
        execSql("CREATE INDEX t1_idx_1 ON " + t(SCHEMA_NAME_4, TBL_NAME) + "(s4_val)");

        execSql("SELECT * FROM " + t(SCHEMA_NAME_1, TBL_NAME));
        execSql("SELECT * FROM " + t(SCHEMA_NAME_2, TBL_NAME));
        execSql("SELECT * FROM " + t(q(SCHEMA_NAME_3), TBL_NAME));
        execSql("SELECT * FROM " + t(SCHEMA_NAME_4, TBL_NAME));

        execSql("SELECT * FROM " + t(SCHEMA_NAME_1, TBL_NAME)
            + " JOIN " + t(SCHEMA_NAME_2, TBL_NAME)
            + " JOIN " + t(q(SCHEMA_NAME_3), TBL_NAME)
            + " JOIN " + t(SCHEMA_NAME_4, TBL_NAME));

        verifyTbls();

        execSql("DROP TABLE " + t(SCHEMA_NAME_1, TBL_NAME));
        execSql("DROP TABLE " + t(SCHEMA_NAME_2, TBL_NAME));
        execSql("DROP TABLE " + t(q(SCHEMA_NAME_3), TBL_NAME));
        execSql("DROP TABLE " + t(SCHEMA_NAME_4, TBL_NAME));
    }

    /** */
    @Test
    public void testRecreateTableWithinSchema() {
        grid(0).cache(CACHE_NAME).destroy();

        grid(0).createCache(new CacheConfiguration<>(CACHE_NAME + "_new").setSqlSchema(SCHEMA_NAME_4));

        List<List<?>> res = execSql("SELECT SQL_SCHEMA FROM " + t(QueryUtils.SCHEMA_SYS, "CACHES")
            + " WHERE CACHE_NAME = '" + CACHE_NAME + "_new'");

        Assert.assertEquals(
            Collections.singletonList(Collections.singletonList(SCHEMA_NAME_4)),
            res
        );
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateTblsInDiffSchemasForSameCache() {
        final String testCache = "cache1";

        execSql("CREATE TABLE " + t(SCHEMA_NAME_1, TBL_NAME)
            + " (s1_key INT PRIMARY KEY, s1_val INT) WITH \"cache_name=" + testCache + "\"");

        GridTestUtils.assertThrowsWithCause(
            () -> execSql("CREATE TABLE " + t(SCHEMA_NAME_2, TBL_NAME)
                + " (s1_key INT PRIMARY KEY, s2_val INT) WITH \"cache_name=" + testCache + "\""),
            SQLException.class
        );

        execSql("DROP TABLE " + t(SCHEMA_NAME_1, TBL_NAME));
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateDropNonExistingSchema() {
        GridTestUtils.assertThrowsWithCause(
            () -> execSql("CREATE TABLE " + t(UNKNOWN_SCHEMA_NAME, TBL_NAME) + "(id INT PRIMARY KEY, val INT)"),
            SQLException.class
        );

        GridTestUtils.assertThrowsWithCause(
            () -> execSql("DROP TABLE " + t(UNKNOWN_SCHEMA_NAME, TBL_NAME)),
            SQLException.class
        );
    }

    /** */
    private void verifyTbls() {
        List<List<?>> res = execSql("SELECT SCHEMA_NAME, KEY_ALIAS FROM " + t(QueryUtils.SCHEMA_SYS, "TABLES") + " ORDER BY SCHEMA_NAME");

        List<List<?>> exp = Arrays.asList(
            Arrays.asList(SCHEMA_NAME_1, "S1_KEY"),
            Arrays.asList(SCHEMA_NAME_2, "S2_KEY"),
            Arrays.asList(SCHEMA_NAME_4, "S4_KEY"),
            Arrays.asList(SCHEMA_NAME_3, "S3_KEY")
        );

        Assert.assertEquals(exp, res);
    }
}

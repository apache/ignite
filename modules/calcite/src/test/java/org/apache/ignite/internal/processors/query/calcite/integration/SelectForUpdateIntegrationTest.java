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

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Integration tests for {@code SELECT ... FOR UPDATE} syntax.
 */
public class SelectForUpdateIntegrationTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrid(0);

        sql("CREATE TABLE Person (id INT PRIMARY KEY, name VARCHAR, age INT)");
        sql("INSERT INTO Person VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Basic FOR UPDATE without options produces "not yet supported". */
    @Test
    public void forUpdateNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person FOR UPDATE");
    }

    /** FOR UPDATE with OF column produces "not yet supported". */
    @Test
    public void forUpdateOfColumnNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person FOR UPDATE OF id");
    }

    /** FOR UPDATE WAIT n produces "not yet supported". */
    @Test
    public void forUpdateWaitNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person FOR UPDATE WAIT 5");
    }

    /** FOR UPDATE NOWAIT produces "not yet supported". */
    @Test
    public void forUpdateNowaitNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person FOR UPDATE NOWAIT");
    }

    /** FOR UPDATE with WHERE clause produces "not yet supported". */
    @Test
    public void forUpdateWithWhereNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person WHERE age > 20 FOR UPDATE");
    }

    /** FOR UPDATE with OF and WAIT produces "not yet supported". */
    @Test
    public void forUpdateOfAndWaitNotYetSupported() {
        assertForUpdateNotSupported("SELECT id FROM Person FOR UPDATE OF name WAIT 3");
    }

    /** Asserts that the given query fails with the "not yet supported" message. */
    private void assertForUpdateNotSupported(String qry) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql(qry), IgniteSQLException.class,
            "SELECT FOR UPDATE is not yet supported");
    }

    /** */
    private java.util.List<java.util.List<?>> sql(String sql, Object... args) {
        return node.context().query().querySqlFields(
            new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), true).getAll();
    }
}

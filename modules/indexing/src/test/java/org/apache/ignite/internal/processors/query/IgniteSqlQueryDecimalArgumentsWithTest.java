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

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgniteSqlQueryDecimalArgumentsWithTest extends GridCommonAbstractTest {
    /** */
    private static final String IDX_NAME = "FLAG_IDX";

    /** */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrid();

        query("create table test_table(id varchar primary key, flag int);");

        query("create index " + IDX_NAME + " on test_table(flag);");

        query("insert into test_table(id, flag) values('a1', 1);");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Math calculations produces decimal values for searching index tree. */
    @Test
    public void testDecimalCalculatedFieldComparedCorrectly() {
        String checkSql = "select * from test_table where flag < 100 * 0.5;";

        assertIndexUsed(checkSql);

        List<List<?>> result = query(checkSql);

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
        assertEquals(1, result.get(0).get(1));
    }

    /** Math calculations produces decimal values for searching index tree. */
    @Test
    public void testExplicitDecimalField() {
        String checkSql = "select * from test_table where flag < ?;";
        BigDecimal arg = new BigDecimal("50.0");

        assertIndexUsed(checkSql, arg);

        List<List<?>> result = query(checkSql, arg);

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
        assertEquals(1, result.get(0).get(1));
    }

    /** Assert if index is not used for searching. */
    private void assertIndexUsed(String sql, Object... args) {
        String explain = "explain " + sql;

        String res = query(explain, args).get(0).get(0).toString();

        assertTrue(Pattern.compile(IDX_NAME + "[^\\s]+: FLAG <").matcher(res).find());
    }

    /** */
    private List<List<?>> query(String qry, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
    }
}

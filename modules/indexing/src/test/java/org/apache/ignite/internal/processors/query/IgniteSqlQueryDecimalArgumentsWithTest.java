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
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Checks whether correctly compare non-inlined decimal search key with inlined types. */
@RunWith(Parameterized.class)
public class IgniteSqlQueryDecimalArgumentsWithTest extends GridCommonAbstractTest {
    /** */
    private static final String IDX_NAME = "FLD_IDX";

    /** */
    private static final String TABLE_NAME = "FLD_TABLE";

    /** */
    private static final String TABLE_CACHE_NAME = "FLD_TABLE_CACHE";

    /** */
    private IgniteEx node;

    /** */
    @Parameterized.Parameter()
    public String idxFldName;

    /** */
    @Parameterized.Parameter(1)
    public String idxFldType;

    /** */
    @Parameterized.Parameters(name = "fld={0} type={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] {"intFld", "int"},
            new Object[] {"dblFld", "double"}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrid();

        query("create table " + TABLE_NAME + "(id varchar primary key, " + idxFldName + " " + idxFldType + ")" +
            " WITH \"CACHE_NAME=" + TABLE_CACHE_NAME + "\";");

        query("create index " + IDX_NAME + " on " + TABLE_NAME + "(" + idxFldName + ");");

        query("insert into " + TABLE_NAME + "(id, " + idxFldName + ") values('a1', 1);");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Math calculations produces decimal values for searching index tree. */
    @Test
    public void testDecimalCalculatedField() {
        check("select * from " + TABLE_NAME + " where " + idxFldName + " < 100 * 0.5;");
    }

    /** */
    @Test
    public void testExplicitDecimalArgument() {
        check("select * from " + TABLE_NAME + " where " + idxFldName + " < ?;", new BigDecimal("50.0"));
    }

    /** */
    private void check(String sql, Object... arg) {
        assertTrue(idxInlineSize() > 0);

        String explain = "explain " + sql;

        String res = query(explain, arg).get(0).get(0).toString();

        // Assert if index is not used for searching.
        assertTrue(Pattern.compile(IDX_NAME + "[^\\s]+: " + idxFldName.toUpperCase() + " <").matcher(res).find());

        List<List<?>> result = query(sql, arg);

        assertEquals(1, result.size());
    }

    /** */
    private List<List<?>> query(String qry, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
    }

    /** */
    private int idxInlineSize() {
        GridCacheContext<?, ?> cctx = node.cachex(TABLE_CACHE_NAME).context();

        InlineIndex idx = (InlineIndex)node.context().indexProcessor().index(
            new IndexName(cctx.name(), "PUBLIC", TABLE_NAME, IDX_NAME));

        return idx.inlineSize();
    }
}

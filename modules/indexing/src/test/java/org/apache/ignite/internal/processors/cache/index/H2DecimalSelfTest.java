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

package org.apache.ignite.internal.processors.cache.index;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.math.RoundingMode.HALF_UP;
import static java.util.Arrays.asList;

/**
 * Test to check decimal columns.
 */
public class H2DecimalSelfTest extends AbstractSchemaSelfTest {
    private static final Integer SCALE = 8;

    private static final Integer PRECISION = 8;

    private static final String COL_NAME = "value";

    private static final String DEC_TAB_NAME = "decimal_table";

    private static final String SALARY_TAB_NAME = "salary";

    private static final MathContext mathCtx = new MathContext(PRECISION);

    private static final BigDecimal VAL_1 = new BigDecimal("123456789", mathCtx).setScale(SCALE, HALF_UP);

    private static final BigDecimal VAL_2 = new BigDecimal("12345678.12345678", mathCtx).setScale(SCALE, HALF_UP);

    private static final BigDecimal VAL_3 = new BigDecimal(".123456789", mathCtx).setScale(SCALE, HALF_UP);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx grid = startGrid(0);

        execute(grid, "CREATE TABLE " + DEC_TAB_NAME +
            "(id LONG PRIMARY KEY, " + COL_NAME + " DECIMAL(" + SCALE + ", " + PRECISION + "))");

        String insertQry = "INSERT INTO " + DEC_TAB_NAME + " VALUES (?, ?)";

        execute(grid, insertQry, 1, VAL_1);
        execute(grid, insertQry, 2, VAL_2);
        execute(grid, insertQry, 3, VAL_3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        Map<String, IgniteBiTuple<Integer, Integer>> decimalInfo = new HashMap<>();

        decimalInfo.put("amount", F.t(SCALE, PRECISION));

        CacheConfiguration<Integer, Salary> ccfg = new CacheConfiguration<>("salary_cache");

        QueryEntity queryEntity = new QueryEntity(Integer.class.getName(), Salary.class.getName());

        queryEntity.setTableName(SALARY_TAB_NAME);

        queryEntity.addQueryField("id", Integer.class.getName(), null);
        queryEntity.addQueryField("amount", BigDecimal.class.getName(), null);

        queryEntity.setDecimalInfo(decimalInfo);

        ccfg.setQueryEntities(Collections.singletonList(queryEntity));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalColumnMetadata() throws Exception {
        checkDecimalInfo(DEC_TAB_NAME, COL_NAME, SCALE, PRECISION);
    }

    public void testQueryEntityInCacheConfig() throws Exception {
        checkDecimalInfo(SALARY_TAB_NAME, "amount", SCALE, PRECISION);
    }

    public void testSelectDecimal() throws Exception {
        IgniteEx grid = grid(0);

        List rows = execute(grid, "SELECT id, value FROM " + DEC_TAB_NAME + " order by id");

        assertEquals(rows.size(), 3);

        assertEquals(asList(1L, VAL_1), rows.get(0));
        assertEquals(asList(2L, VAL_2), rows.get(1));
        assertEquals(asList(3L, VAL_3), rows.get(2));
    }

    private void checkDecimalInfo(String tabName, String colName, Integer scale, Integer precision) {
        QueryEntity queryEntity = findTableInfo(tabName);

        assertNotNull(queryEntity);

        Map<String, IgniteBiTuple<Integer, Integer>> decimalInfo = queryEntity.getDecimalInfo();

        assertNotNull(decimalInfo);

        IgniteBiTuple<Integer, Integer> columnInfo = decimalInfo.get(colName);

        assertNotNull(columnInfo);

        assertEquals(columnInfo.get1(), scale);
        assertEquals(columnInfo.get2(), precision);
    }

    /**
     * @param tabName Table name.
     * @return QueryEntity of table.
     */
    private QueryEntity findTableInfo(String tabName) {
        IgniteEx ignite = grid(0);

        Collection<String> cacheNames = ignite.cacheNames();

        for (String cacheName : cacheNames) {
            CacheConfiguration ccfg = ignite.cache(cacheName).getConfiguration(CacheConfiguration.class);

            Collection<QueryEntity> entities = ccfg.getQueryEntities();

            for (QueryEntity entity : entities)
                if (entity.getTableName().equalsIgnoreCase(tabName))
                    return entity;
        }

        return null;
    }

    /**
     * Execute DDL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private List<List<?>> execute(Ignite node, String sql, Object...args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }

    private static class Salary {
        private BigDecimal amount;

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }
    }
}

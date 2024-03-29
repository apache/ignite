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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 * Test to check decimal columns.
 */
public class IgniteDecimalSelfTest extends AbstractSchemaSelfTest {
    /** */
    private static final int PRECISION = 9;

    /** */
    private static final int SCALE = 8;

    /** */
    private static final String DEC_TAB_NAME = "DECIMAL_TABLE";

    /** */
    private static final String VALUE = "VALUE";

    /** */
    private static final String SALARY_TAB_NAME = "SALARY";

    /** */
    private static final MathContext MATH_CTX = new MathContext(PRECISION);

    /** */
    private static final BigDecimal VAL_1 = BigDecimal.valueOf(123456789);

    /** */
    private static final BigDecimal VAL_2 = BigDecimal.valueOf(1.23456789);

    /** */
    private static final BigDecimal VAL_3 = BigDecimal.valueOf(.12345678);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx grid = startGrid(0);

        execute(grid, "CREATE TABLE " + DEC_TAB_NAME +
            "(id LONG PRIMARY KEY, " + VALUE + " DECIMAL(" + PRECISION + ", " + SCALE + "))");

        String insertQry = "INSERT INTO " + DEC_TAB_NAME + " VALUES (?, ?)";

        execute(grid, insertQry, 1, VAL_1);
        execute(grid, insertQry, 2, VAL_2);
        execute(grid, insertQry, 3, VAL_3);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Salary> ccfg = cacheCfg(SALARY_TAB_NAME, "salary_cache");

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @NotNull private CacheConfiguration<Integer, Salary> cacheCfg(String tabName, String cacheName) {
        CacheConfiguration<Integer, Salary> ccfg = new CacheConfiguration<>(cacheName);

        QueryEntity qryEntity = new QueryEntity(Integer.class.getName(), Salary.class.getName());

        qryEntity.setTableName(tabName);

        qryEntity.addQueryField("id", Integer.class.getName(), null);
        qryEntity.addQueryField("amount", BigDecimal.class.getName(), null);

        Map<String, Integer> precision = new HashMap<>();
        Map<String, Integer> scale = new HashMap<>();

        precision.put("amount", PRECISION);
        scale.put("amount", SCALE);

        qryEntity.setFieldsPrecision(precision);
        qryEntity.setFieldsScale(scale);

        ccfg.setQueryEntities(Collections.singletonList(qryEntity));

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfiguredFromDdl() throws Exception {
        checkPrecisionAndScale(DEC_TAB_NAME, VALUE, PRECISION, SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfiguredFromQueryEntity() throws Exception {
        checkPrecisionAndScale(SALARY_TAB_NAME, "amount", PRECISION, SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfiguredFromQueryEntityInDynamicallyCreatedCache() throws Exception {
        IgniteEx grid = grid(0);

        String tabName = SALARY_TAB_NAME + "2";

        CacheConfiguration<Integer, Salary> ccfg = cacheCfg(tabName, "SalaryCache-2");

        IgniteCache<Integer, Salary> cache = grid.createCache(ccfg);

        checkPrecisionAndScale(tabName, "amount", PRECISION, SCALE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfiguredFromAnnotations() throws Exception {
        IgniteEx grid = grid(0);

        CacheConfiguration<Integer, Salary> ccfg = new CacheConfiguration<>("SalaryCache-3");

        ccfg.setIndexedTypes(Integer.class, SalaryWithAnnotations.class);

        grid.createCache(ccfg);

        checkPrecisionAndScale(SalaryWithAnnotations.class.getSimpleName().toUpperCase(), "amount", PRECISION, SCALE);
    }

    /** */
    @Test
    public void testSelectDecimal() throws Exception {
        IgniteEx grid = grid(0);

        List rows = execute(grid, "SELECT id, value FROM " + DEC_TAB_NAME + " order by id");

        assertEquals(rows.size(), 3);

        assertEquals(asList(1L, VAL_1), rows.get(0));
        assertEquals(asList(2L, VAL_2), rows.get(1));
        assertEquals(asList(3L, VAL_3), rows.get(2));
    }

    /** */
    private void checkPrecisionAndScale(String tabName, String colName, Integer precision, Integer scale) {
        QueryEntity qryEntity = findTableInfo(tabName);

        assertNotNull(qryEntity);

        Map<String, Integer> fieldsPrecision = qryEntity.getFieldsPrecision();

        assertNotNull(precision);

        assertEquals(fieldsPrecision.get(colName), precision);

        Map<String, Integer> fieldsScale = qryEntity.getFieldsScale();

        assertEquals(fieldsScale.get(colName), scale);

        assertNotNull(scale);
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
    private List<List<?>> execute(Ignite node, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }

    /** */
    private static class Salary {
        /** */
        private BigDecimal amount;

        /** */
        public BigDecimal getAmount() {
            return amount;
        }

        /** */
        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }
    }

    /** */
    private static class SalaryWithAnnotations {
        /** */
        @QuerySqlField(index = true, precision = PRECISION, scale = SCALE)
        private BigDecimal amount;

        /** */
        public BigDecimal getAmount() {
            return amount;
        }

        /** */
        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }
    }
}

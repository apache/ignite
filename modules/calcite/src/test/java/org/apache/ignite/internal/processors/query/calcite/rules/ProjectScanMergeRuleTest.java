/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rules;

import static java.util.Collections.singletonList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsAnyProject;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.notContainsProject;
import static org.apache.ignite.internal.processors.query.calcite.rules.OrToUnionRuleTest.Product;

/**
 * Tests projection rule {@code org.apache.ignite.internal.processors.query.calcite.rule.ProjectScanMergeRule}
 * This rule have a deal with only useful columns and.
 * For example for tables: T1(f12, f12, f13) and T2(f21, f22, f23)
 * sql execution: SELECT t1.f11, t2.f21 FROM T1 t1 INNER JOIN T2 t2 on t1.f11 = t2.f22"
 * need to eleminate all unused coluns and take into account only: f11, f21 and f22 cols.
 */
public class ProjectScanMergeRuleTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(2);

        QueryEntity qryEnt = new QueryEntity();
        qryEnt.setKeyFieldName("ID");
        qryEnt.setKeyType(Integer.class.getName());
        qryEnt.setValueType(Product.class.getName());

        qryEnt.addQueryField("ID", Integer.class.getName(), null);
        qryEnt.addQueryField("CATEGORY", String.class.getName(), null);
        qryEnt.addQueryField("CAT_ID", Integer.class.getName(), null);
        qryEnt.addQueryField("SUBCATEGORY", String.class.getName(), null);
        qryEnt.addQueryField("SUBCAT_ID", Integer.class.getName(), null);
        qryEnt.addQueryField("NAME", String.class.getName(), null);

        qryEnt.setTableName("products");

        final CacheConfiguration<Integer, Product> cfg = new CacheConfiguration<>(qryEnt.getTableName());

        cfg.setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(qryEnt))
            .setSqlSchema("PUBLIC");

        IgniteCache<Integer, Product> devCache = grid.createCache(cfg);

        devCache.put(1, new Product(1, "prod1", 1, "cat1", 11, "noname1"));
        devCache.put(2, new Product(2, "prod2", 2, "cat1", 11, "noname2"));
        devCache.put(3, new Product(3, "prod3", 3, "cat1", 12, "noname3"));

        awaitPartitionMapExchange();
    }

    /** */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(grid(0).context(), QueryEngine.class);
            }
        };
    }

    /**
     * Tests that the projects exist only for simple expressions without any predicates.
     */
    @Test
    public void testProjects() {
        checkQuery("SELECT name FROM products d;")
            .matches(containsScan("PUBLIC", "PRODUCTS"))
            .matches(containsAnyProject("PUBLIC", "PRODUCTS"))
            .returns("noname1")
            .returns("noname2")
            .returns("noname3")
            .check();

        checkQuery("SELECT name FROM products d WHERE CAT_ID > 1;")
            .matches(containsScan("PUBLIC", "PRODUCTS"))
            .matches(notContainsProject("PUBLIC", "PRODUCTS"))
            .returns("noname2")
            .returns("noname3")
            .check();
    }
}

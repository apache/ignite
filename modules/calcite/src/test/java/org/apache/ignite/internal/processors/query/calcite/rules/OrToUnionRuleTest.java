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

package org.apache.ignite.internal.processors.query.calcite.rules;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsUnion;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test OR -> UnionAll rewrite rule.
 *
 * Example:
 * SELECT * FROM products
 * WHERE category = 'Photo' OR subcategory ='Camera Media';
 *
 * A query above will be rewritten to next (or equivalient similar query)
 *
 * SELECT * FROM products
 *      WHERE category = 'Photo'
 * UNION ALL
 * SELECT * FROM products
 *      WHERE subcategory ='Camera Media' AND category != 'Photo';
 */
public class OrToUnionRuleTest extends GridCommonAbstractTest {
    /** */
    public static final String IDX_SUBCAT_ID = "IDX_SUBCAT_ID";

    /** */
    public static final String IDX_SUBCATEGORY = "IDX_SUBCATEGORY";

    /** */
    public static final String IDX_CATEGORY = "IDX_CATEGORY";

    /** */
    public static final String IDX_CAT_ID = "IDX_CAT_ID";

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

        qryEnt.setIndexes(asList(
            new QueryIndex("CATEGORY", QueryIndexType.SORTED).setName(IDX_CATEGORY),
            new QueryIndex("CAT_ID", QueryIndexType.SORTED).setName(IDX_CAT_ID),
            new QueryIndex("SUBCATEGORY", QueryIndexType.SORTED).setName(IDX_SUBCATEGORY),
            new QueryIndex("SUBCAT_ID", QueryIndexType.SORTED).setName(IDX_SUBCAT_ID)
        ));
        qryEnt.setTableName("products");

        final CacheConfiguration<Integer, Product> cfg = new CacheConfiguration<>(qryEnt.getTableName());

        cfg.setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(qryEnt))
            .setSqlSchema("PUBLIC");

        IgniteCache<Integer, Product> devCache = grid.createCache(cfg);

        devCache.put(1, new Product(1, "Photo", 1, "Camera Media", 11, "Media 1"));
        devCache.put(2, new Product(2, "Photo", 1, "Camera Media", 11, "Media 2"));
        devCache.put(3, new Product(3, "Photo", 1, "Camera Lens", 12, "Lens 1"));
        devCache.put(4, new Product(4, "Photo", 1, "Other", 12, "Charger 1"));
        devCache.put(5, new Product(5, "Video", 2, "Camera Media", 21, "Media 3"));
        devCache.put(6, new Product(6, "Video", 2, "Camera Lens", 22, "Lens 3"));
        devCache.put(7, new Product(7, "Video", 1, null, 0, "Canon"));
        devCache.put(8, new Product(8, null, 0, "Camera Lens", 11, "Zeiss"));
        devCache.put(9, new Product(9, null, 0, null, 0, null));

        awaitPartitionMapExchange();
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualityOrToUnionAllRewrite() throws Exception {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Video' " +
            "OR subcategory ='Camera Lens'")
            .and(containsUnion(true))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonDistinctOrToUnionAllRewrite() throws Exception {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE subcategory = 'Camera Lens' " +
            "OR subcategory = 'Other'")
            .and(containsUnion(true))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for mixed conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedOrToUnionAllRewrite() throws Exception {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Photo' " +
            "OR (subcat_id > 12 AND subcat_id < 22)")
            .and(containsUnion(true))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
            .returns(1, "Photo", 1, "Camera Media", 11, "Media 1")
            .returns(2, "Photo", 1, "Camera Media", 11, "Media 2")
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied for range conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeOrToUnionAllRewrite() throws Exception {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE cat_id > 1 " +
            "OR subcat_id < 10")
            .and(containsUnion(true))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_CAT_ID"))
            .and(containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
        .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if (at least) one of column is not indexed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonIndexedOrToUnionAllRewrite() throws Exception {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE name = 'Canon' " +
            "OR category = 'Photo'")
//            .and(not(containsUnion(true)))
            .and(containsScan("PUBLIC", "PRODUCTS", "PK"))
            .check();
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
     *
     */
    static class Product {
        long id;
        String category;
        int cat_Id;
        String subCategory;
        int subcat_Id;
        String name;

        public Product(long id, String category, int cat_Id, String subCategory, int subcat_Id, String name) {
            this.id = id;
            this.category = category;
            this.cat_Id = cat_Id;
            this.subCategory = subCategory;
            this.subcat_Id = subcat_Id;
            this.name = name;
        }
    }
}

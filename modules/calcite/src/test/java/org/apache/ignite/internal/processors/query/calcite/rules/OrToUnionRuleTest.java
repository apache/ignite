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
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsTableScan;
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
 *      WHERE subcategory ='Camera Media' AND LNNVL(category, 'Photo');
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
            .setBackups(0)
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
        devCache.put(10, new Product(10, null, 0, null, 30, null));
        devCache.put(11, new Product( 11, null, 0, null, 30, null));
        devCache.put(12, new Product( 12, null, 0, null, 31, null));
        devCache.put(13, new Product( 13, null, 0, null, 31, null));

        devCache.put(14, new Product( 14, null, 0, null, 32, null));
        devCache.put(15, new Product( 15, null, 0, null, 33, null));
        devCache.put(16, new Product( 16, null, 0, null, 34, null));
        devCache.put(17, new Product( 17, null, 0, null, 35, null));
        devCache.put(18, new Product( 18, null, 0, null, 36, null));
        devCache.put(19, new Product( 19, null, 0, null, 37, null));
        devCache.put(20, new Product( 20, null, 0, null, 38, null));
        devCache.put(21, new Product( 21, null, 0, null, 39, null));
        devCache.put(22, new Product( 22, null, 0, null, 40, null));
        devCache.put(23, new Product( 23, null, 0, null, 41, null));

        awaitPartitionMapExchange();
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     */
    @Test
    public void testEqualityOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Video' " +
            "OR subcategory ='Camera Lens'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is NOT applied for conditions on the same indexed column.
     */
    @Test
    public void testNonDistinctOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE subcategory = 'Camera Lens' " +
            "OR subcategory = 'Other'")
            .matches(CoreMatchers.not(containsUnion(true)))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for mixed conditions on indexed columns.
     */
    @Test
    public void testMixedOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Photo' " +
            "OR (subcat_id > 12 AND subcat_id < 22)")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
            .returns(1, "Photo", 1, "Camera Media", 11, "Media 1")
            .returns(2, "Photo", 1, "Camera Media", 11, "Media 2")
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is NOT applied if no acceptable index was found.
     */
    @Test
    public void testUnionRuleNotApplicable() {
        checkQuery("SELECT * FROM products WHERE name = 'Canon' OR subcat_id = 22")
            .matches(CoreMatchers.not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .check();
    }

    /**
     * Check request with hidden keys.
     */
    @Test
    public void testWithHiddenKeys() {
        checkQuery("SELECT _key, _val FROM products WHERE category = 'Photo' OR subcat_id = 22")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied for range conditions on indexed columns.
     */
    @Test
    public void testRangeOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE cat_id > 1 " +
            "OR subcat_id < 10 ")
            .matches(not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(9, null, 0, null, 0, null)
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if (at least) one of column is not indexed.
     */
    @Test
    public void testNonIndexedOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE name = 'Canon' " +
            "OR category = 'Video'")
            .matches(not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if all columns are not indexed.
     */
    @Test
    public void testAllNonIndexedOrToUnionAllRewrite() {
        checkQuery("SELECT * " +
            "FROM products " +
            "WHERE name = 'Canon' " +
            "OR name = 'Sony'")
            .matches(not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(7, "Video", 1, null, 0, "Canon")
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
    public static class Product {
        /** */
        long id;

        /** */
        String category;

        /** */
        int cat_Id;

        /** */
        String subCategory;

        /** */
        int subcat_Id;

        /** */
        String name;

        /** Constructor. */
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

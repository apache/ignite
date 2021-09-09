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

package org.apache.ignite.cache.query;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.cache.IndexFieldOrder;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexNullsOrderTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx ignite;

    /** */
    private static final String CACHE = "PRICE";

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(0);

        query("create table A (id int primary key, price int) " +
            "with \"CACHE_NAME=" + CACHE + ",VALUE_TYPE=" + Price.class.getName() + "\"");

        query("insert into A (id, price) values (0, null)");
        query("insert into A (id, price) values (1, 10)");
        query("insert into A (id, price) values (2, 30)");
        query("insert into A (id, price) values (3, null)");
    }

    /** */
    @Test
    public void tessAscIndex() {
        String idxName = createIndex(new IndexFieldOrder(true));

        checkIndexQuery(idxName, F.asList(0, 3, 1, 2));
    }

    /** */
    @Test
    public void tessDescIndex() {
        String idxName = createIndex(new IndexFieldOrder(false));

        // DESC is only for PRICE field, _KEY is ASC.
        checkIndexQuery(idxName, F.asList(2, 1, 0, 3));
    }

    /** */
    @Test
    public void tessAscNullsFirstIndex() {
        String idxName = createIndex(new IndexFieldOrder(true, true, false));

        checkIndexQuery(idxName, F.asList(0, 3, 1, 2));
    }

    /** */
    @Test
    public void tessAscNullsLastIndex() {
        String idxName = createIndex(new IndexFieldOrder(true, false, true));

        checkIndexQuery(idxName, F.asList(1, 2, 0, 3));
    }

    /** */
    @Test
    public void tessDescNullsFirstIndex() {
        String idxName = createIndex(new IndexFieldOrder(false, true, false));

        checkIndexQuery(idxName, F.asList(0, 3, 2, 1));
    }

    /** */
    @Test
    public void tessDescNullsLastIndex() {
        String idxName = createIndex(new IndexFieldOrder(false, false, true));

        checkIndexQuery(idxName, F.asList(2, 1, 0, 3));
    }

    /** */
    private String createIndex(IndexFieldOrder order) {
        String o = order.isAscending() ? "asc" : "desc";

        if (order.isNullsFirst())
            o += " nulls first";
        else if (order.isNullsLast())
            o += " nulls last";

        String idxName = "PRICE_" + o.replace(" ", "_") + "_IDX";

        query("create index " + idxName + " on A (price " + o + ")");

        return idxName;
    }

    /** */
    private void checkIndexQuery(String idxName, List<Integer> expOrder) {
        List<Cache.Entry<Integer, Price>> result = ignite.cache(CACHE)
            .query(new IndexQuery<Integer, Price>(Price.class, idxName)
                .setCriteria(lt("price", Integer.MAX_VALUE)))
            .getAll();

        for (int i = 0; i < expOrder.size(); i++) {
            assertEquals("Wrong order for " + idxName + ". Exp=" + expOrder + "; Act= " + result,
                expOrder.get(i), result.get(i).getKey());
        }
    }

    /** */
    private List<List<?>> query(String qry) {
        return ignite.context().query()
            .querySqlFields(new SqlFieldsQuery(qry), false)
            .getAll();
    }

    /** */
    private static class Price {
        /** */
        @QuerySqlField
        private Integer id;

        /** */
        @QuerySqlField
        private Integer price;

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Price[id=" + id + "; price=" + price + "]";
        }
    }
}

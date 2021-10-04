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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class ComplexIndexNullsOrderTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "CACHE";

    /** */
    private static final String VALUE_TYPE = "VALUE_TYPE";

    /** */
    @Test
    public void testNullPriceOtherIndex() throws Exception {
        startGrid(0);

        query("create table A(id int primary key, price int, other int) " +
            "with \"CACHE_NAME=" + CACHE + ",VALUE_TYPE=" + VALUE_TYPE + "\"");

        query("create index IDX on A (price, other)");

        Set<Integer> keys = new TreeSet<>();

        Random r = new Random();

        for (int i = 0; i < 1000; i++) {
            int v = r.nextInt(10000);

            while (keys.contains(v))
                v = r.nextInt(10000);

            keys.add(v);

            query("insert into A(id, price, other) values (?, null, ?)", i, v);
        }

        IgniteCache<Integer, BinaryObject> cache = grid(0).cache(CACHE).withKeepBinary();

        Iterator<Cache.Entry<Integer, BinaryObject>> cursor = cache
            .query(new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, "IDX")
                .setCriteria(lt("price", Integer.MAX_VALUE), lt("other", Integer.MAX_VALUE)))
            .iterator();

        keys.iterator().forEachRemaining(v -> {
            Cache.Entry<Integer, BinaryObject> e = cursor.next();

            assertEquals(v, e.getValue().field("other"));
        });
    }

    /** */
    @Test
    public void testNullPriceKeyIndex() throws Exception {
        startGrid(0);

        query("create table A(id int primary key, price int, other int) " +
            "with \"CACHE_NAME=" + CACHE + ",VALUE_TYPE=" + VALUE_TYPE + "\"");

        query("create index IDX_PRICE on A (price)");

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        Random r = new Random();

        for (int i = 0; i < 1000; i++) {
            int pos = r.nextInt(keys.size());

            int k = keys.remove(pos);

            query("insert into A(id, price, other) values (?, null, null)", k);
        }

        IgniteCache<Integer, BinaryObject> cache = grid(0).cache(CACHE).withKeepBinary();

        List<Cache.Entry<Integer, BinaryObject>> cursor = cache
            .query(new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, "IDX_PRICE")
                .setCriteria(lt("price", Integer.MAX_VALUE)))
            .getAll();

        for (int i = 0; i < 1000; i++)
            assertEquals(i, cursor.get(i).getKey().intValue());
    }

    /** */
    private List<List<?>> query(String qry, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false)
            .getAll();
    }
}

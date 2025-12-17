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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.cache.query.TextQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** */
public class GridCacheFullTextQueryLimitTest extends GridCacheFullTextQueryAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_PER_NODE_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @ParameterizedTest(name = "nodesCnt={0}")
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
    public void testResultOrderedByScore(int nodesCnt) throws Exception {
        startGrids(nodesCnt);

        int items = MAX_ITEM_PER_NODE_COUNT * nodesCnt;

        int helloPivot = new Random().nextInt(items);

        for (int i = 0; i < items; i++) {
            String name = i == helloPivot ? "hello" : String.format("hello%02d", i);

            cache().put(i, new Person(name));
        }

        // Extract all data that matches even little.
        for (int limit = 1; limit <= items; limit++) {
            TextQuery<Integer, Person> qry = new TextQuery<Integer, Person>(Person.class, "hello~")
                .setLimit(limit);

            List<Cache.Entry<Integer, Person>> result = cache().query(qry).getAll();

            // Lucene returns top 50 results by query from single node even if limit is higher.
            // Number of docs depends on amount of data on every node.
            if (limit <= 50)
                assertEquals(limit, result.size());
            else
                assertTrue(limit >= result.size());

            // hello has to be on the top.
            assertEquals("Limit=" + limit, "hello", result.get(0).getValue().name);
        }
    }
}

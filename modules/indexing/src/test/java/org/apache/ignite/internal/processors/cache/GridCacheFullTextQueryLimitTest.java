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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/** */
@RunWith(Parameterized.class)
public class GridCacheFullTextQueryLimitTest extends GridCommonAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_PER_NODE_COUNT = 100;

    /** Cache name */
    private static final String PERSON_CACHE = "Person";

    /** Number of nodes. */
    @Parameterized.Parameter(0)
    public int nodesCnt;

    /** */
    @Parameterized.Parameters(name = "nodesCnt={0}")
    public static Iterable<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (int i = 1; i <= 8; i++) {
            Object[] p = new Object[1];
            p[0] = i;

            params.add(p);
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(PERSON_CACHE)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** */
    @Test
    public void testResultOrderedByScore() throws Exception {
        startGrids(nodesCnt);

        int items = MAX_ITEM_PER_NODE_COUNT * nodesCnt;

        final IgniteEx ignite = grid(0);

        IgniteCache<Integer, Person> cache = ignite.cache(PERSON_CACHE);

        int helloPivot = new Random().nextInt(items);

        for (int i = 0; i < items; i++) {
            String name = i == helloPivot ? "hello" : String.format("hello%02d", i);

            cache.put(i, new Person(name));
        }

        // Extract all data that matches even little.
        for (int limit = 1; limit <= items; limit++) {
            TextQuery qry = new TextQuery<>(Person.class, "hello~")
                .setLimit(limit);

            List<Cache.Entry<Integer, Person>> result = cache.query(qry).getAll();

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

    /**
     * Test model class.
     */
    public static class Person implements Serializable {
        /** */
        @QueryTextField
        String name;

        /** */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person{" +
                "name='" + name + '\'' +
                '}';
        }
    }
}

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
package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Limit / offset tests.
 */
public class LimitOffsetTest extends GridCommonAbstractTest {
    /** */
    private static final int ROWS = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGrids(2);

        QueryEntity e = new QueryEntity()
            .setTableName("Test")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);

        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(e);

        IgniteCache<Integer, String> cache = grid.createCache(ccfg);

        for (int i = 0; i < ROWS; ++i)
            cache.put(i, "val_" + 1);
    }

    /** */
    private CacheConfiguration cacheConfiguration(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    /** */
    @Test
    public void test() {
        // Check plan.
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        // Check result.
        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", "SELECT * FROM TEST " +
                "OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY", X.EMPTY_OBJECT_ARRAY);

        FieldsQueryCursor<List<?>> cur = cursors.get(0);

        List<List<?>> res = cur.getAll();

        res.forEach(System.out::println);
    }
}

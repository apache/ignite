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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Sort aggregate integration test.
 */
public class SortAggregateIntegrationTest extends GridCommonAbstractTest {
    /** */
    public static final int ROWS = 103;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fillCache(grid(0).cache("TEST"), ROWS);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache("TEST").clear();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        QueryEntity part = new QueryEntity()
            .setTableName("TEST")
            .setKeyType(Integer.class.getName())
            .setValueType(TestVal.class.getName())
            .setKeyFieldName("ID")
            .addQueryField("ID", Integer.class.getName(), null)
            .addQueryField("GRP0", Integer.class.getName(), null)
            .addQueryField("GRP1", Integer.class.getName(), null)
            .addQueryField("VAL0", Integer.class.getName(), null)
            .addQueryField("VAL1", Integer.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex(Arrays.asList("GRP0", "GRP1"), QueryIndexType.SORTED)));

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(part.getTableName())
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(singletonList(part))
                    .setSqlSchema("PUBLIC")
            );
    }

    /** */
    @Test
    public void mapReduceAggregate() {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors = engine.query(
            null,
            "PUBLIC",
            "SELECT /*+ DISABLE_RULE('HashAggregateConverterRule') */" +
                "SUM(val0), SUM(val1), grp0 FROM TEST " +
                "GROUP BY grp0 " +
                "HAVING SUM(val1) > 10",
            X.EMPTY_OBJECT_ARRAY
        );

        List<List<?>> res = cursors.get(0).getAll();

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            BigDecimal s0 = (BigDecimal)r.get(0);
            BigDecimal s1 = (BigDecimal)r.get(1);

            assertEquals(s0.multiply(new BigDecimal(2)), s1);
        });
    }

    /**
     * @param c Cache.
     * @param rows Rows count.
     */
    private void fillCache(IgniteCache c, int rows) throws InterruptedException {
        c.clear();

        for (int i = 0; i < rows; ++i)
            c.put(i, new TestVal(i));

        awaitPartitionMapExchange();
    }

    /** */
    public static class TestVal {
        /** */
        int grp0;

        /** */
        int grp1;

        /** */
        int val0;

        /** */
        int val1;

        /** */
        TestVal(int k) {
            grp0 = k / 10;
            grp1 = k / 100;

            val0 = 1;
            val1 = 2;
        }
    }
}

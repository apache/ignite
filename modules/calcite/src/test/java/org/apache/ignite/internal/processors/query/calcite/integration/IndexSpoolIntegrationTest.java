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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


/**
 * Index spool test.
 */
@RunWith(Parameterized.class)
public class IndexSpoolIntegrationTest extends GridCommonAbstractTest {
    /** Rows. */
    private static final int[] ROWS = {1, 10, 512, 513, 2000};

    /** */
    @Parameterized.Parameter(0)
    public int rows;

    /**
     * @return List of versions pairs to test.
     */
    @Parameterized.Parameters(name = "Rows: {0}")
    public static Collection<Object[]> testData() {
        List<Object[]> params = new ArrayList<>();

        for (int rs : ROWS)
            params.add(new Object[]{rs});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fillCache(grid(0).cache("TEST0"), rows);
        fillCache(grid(0).cache("TEST1"), rows);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache("TEST0").clear();
        grid(0).cache("TEST1").clear();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        QueryEntity part0 = new QueryEntity()
            .setTableName("TEST0")
            .setKeyType(Integer.class.getName())
            .setValueType(TestVal.class.getName())
            .setKeyFieldName("ID")
            .addQueryField("ID", Integer.class.getName(), null)
            .addQueryField("JID", Integer.class.getName(), null)
            .addQueryField("VAL", String.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex("JID")));

        QueryEntity part1 = new QueryEntity()
            .setTableName("TEST1")
            .setKeyType(Integer.class.getName())
            .setValueType(TestVal.class.getName())
            .setKeyFieldName("ID")
            .addQueryField("ID", Integer.class.getName(), null)
            .addQueryField("JID", Integer.class.getName(), null)
            .addQueryField("VAL", String.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex("JID")));

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(part0.getTableName())
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(singletonList(part0))
                    .setSqlSchema("PUBLIC"),
                new CacheConfiguration<>(part1.getTableName())
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(singletonList(part1))
                    .setSqlSchema("PUBLIC")
            );
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors = engine.query(
            null,
            "PUBLIC",
            "SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */" +
                "T0.val, T1.val FROM TEST0 as T0 " +
                "JOIN TEST1 as T1 on T0.jid = T1.jid ",
            X.EMPTY_OBJECT_ARRAY
        );

        List<List<?>> res = cursors.get(0).getAll();

        assertThat(res.size(), is(rows));

        res.forEach(r -> assertThat(r.get(0), is(r.get(1))));
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
        int jid;

        /** */
        String val;

        /** */
        TestVal(int k) {
            jid = k + 1;
            val = "val_" + k;
        }
    }
}

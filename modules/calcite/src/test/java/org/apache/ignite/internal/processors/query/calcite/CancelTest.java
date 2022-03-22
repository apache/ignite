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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.metadata.RemoteException;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.query.QueryCancelledException.ERR_MSG;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.awaitReservationsRelease;

/**
 * Cancel query test.
 */
public class CancelTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);

        IgniteCache<Integer, String> c = grid(0).cache("TEST");

        fillCache(c, 5000);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        QueryEntity ePart = new QueryEntity()
            .setTableName("TEST")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);;

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(ePart.getTableName())
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(singletonList(ePart))
                    .setSqlSchema("PUBLIC"));
    }

    /**
     *
     */
    @Test
    public void testCancel() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC",
                "SELECT * FROM TEST",
                X.EMPTY_OBJECT_ARRAY);

        Iterator<List<?>> it = cursors.get(0).iterator();

        it.next();

        cursors.forEach(QueryCursor::close);

        GridTestUtils.assertThrows(log, () -> {
                it.next();

                return null;
            },
            IgniteSQLException.class, ERR_MSG
        );

        awaitReservationsRelease("TEST");
    }

    /**
     *
     */
    @Test
    public void testNotOriginatorNodeStop() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC",
                "SELECT * FROM TEST",
                X.EMPTY_OBJECT_ARRAY);

        Iterator<List<?>> it = cursors.get(0).iterator();

        it.next();

        stopGrid(1);

        Throwable ex = GridTestUtils.assertThrows(log, () -> {
            while (it.hasNext())
                it.next();

            return null;
        }, IgniteSQLException.class, null);

        // Sometimes remote node during stopping can send error to originator node and this error processed before
        // node left event, in this case exception stack will looks like:
        // IgniteSQLException -> RemoteException -> IgniteInterruptedCheckedException
        if (!X.hasCause(ex, "node left", ClusterTopologyCheckedException.class) && !(X.hasCause(ex,
            RemoteException.class) && X.hasCause(ex, IgniteInterruptedCheckedException.class))) {
            log.error("Unexpected exception", ex);

            fail("Unexpected exception: " + ex);
        }

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), 10_000));

        awaitReservationsRelease(grid(0), "TEST");
    }

    /**
     *
     */
    @Test
    public void testOriginatorNodeStop() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC",
                "SELECT * FROM TEST",
                X.EMPTY_OBJECT_ARRAY);

        Iterator<List<?>> it = cursors.get(0).iterator();

        it.next();

        stopGrid(0);

        QueryEngine engine1 = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine1.runningQueries().isEmpty(), 10_000));

        awaitReservationsRelease(grid(1), "TEST");
    }

    /**
     *
     */
    @Test
    public void testReadToEnd() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC",
                "SELECT * FROM TEST WHERE ID < 1",
                X.EMPTY_OBJECT_ARRAY);

        Iterator<List<?>> it = cursors.get(0).iterator();

        it.next();

        GridTestUtils.assertThrows(log, () -> {
                it.next();

                return null;
            },
            NoSuchElementException.class, null
        );

        GridTestUtils.assertThrows(log, () -> {
                it.next();

                return null;
            },
            NoSuchElementException.class, null
        );

        // Checks that all partition are unreserved.
        awaitReservationsRelease("TEST");
    }

    /**
     *
     */
    @Test
    public void testFullReadToEnd() throws Exception {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC",
                "SELECT * FROM TEST WHERE ID < 1",
                X.EMPTY_OBJECT_ARRAY);

        cursors.get(0).getAll();

        // Checks that all partition are unreserved.
        awaitReservationsRelease("TEST");
    }

    /**
     * @param c Cache.
     * @param rows Rows count.
     */
    private void fillCache(IgniteCache c, int rows) throws InterruptedException {
        c.clear();

        for (int i = 0; i < rows; ++i)
            c.put(i, "val_" + i);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    private void startNewNode() throws Exception {
        startGrid(2);

        awaitPartitionMapExchange();
    }
}

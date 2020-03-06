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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class KillQueryErrorOnCancelTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(2);

        for (int i = 0; i < 1000; ++i)
            grid(1).cache(GridAbstractTest.DEFAULT_CACHE_NAME).put(i, i);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = GridAbstractTest.defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(0);
        cache.setIndexedTypes(Integer.class, Integer.class);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) {
                if (GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                    Message gridMsg = ((GridIoMessage)msg).message();

                    if (gridMsg instanceof GridQueryCancelRequest)
                        throw new RuntimeException("Fake network error");
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testCancelAfterIteratorObtainedLazy() throws Exception {
        IgniteEx node = grid(0);

        FieldsQueryCursor<List<?>> cur = node.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer ORDER BY _val")
                .setLazy(true)
                .setPageSize(1), false);

        Iterator<List<?>> it = cur.iterator();

        it.next();

        Long qryId = node.context().query().runningQueries(-1).iterator().next().id();

        GridTestUtils.assertThrows(log,
            () -> node.context().query()
            .querySqlFields(createKillQuery(node.context().localNodeId(), qryId, false), false)
            .getAll(),
            IgniteException.class, "Fake network error");

        List<GridRunningQueryInfo> runningQueries =
            (List<GridRunningQueryInfo>)node.context().query().runningQueries(-1);

        assertTrue("runningQueries=" + runningQueries, runningQueries.isEmpty());
        ensureMapQueriesHasFinished(grid(0));
        ensureMapQueriesHasFinished(grid(1));
    }

    /**
     * Wait until all map parts are finished on the specified node. Not needed when IGN-13862 is done.
     *
     * @param node node for which map request completion to wait.
     */
    private void ensureMapQueriesHasFinished(IgniteEx node) throws Exception {
        boolean noTasksInQryPool = GridTestUtils.waitForCondition(() -> queryPoolIsEmpty(node), 5_000);

        Assert.assertTrue("Node " + node.localNode().id() + " has not finished its tasks in the query pool",
            noTasksInQryPool);
    }

    /**
     * @param node node which query pool to check.
     * @return {@code True} if {@link GridIoPolicy#QUERY_POOL} is empty. This means no queries are currntly executed and
     * no queries are executed at the moment; {@code false} otherwise.
     */
    private boolean queryPoolIsEmpty(IgniteEx node) {
        ThreadPoolExecutor qryPool = (ThreadPoolExecutor)node.context().getQueryExecutorService();

        return qryPool.getQueue().isEmpty() && qryPool.getActiveCount() == 0;
    }

    /**
     * @param nodeId Node id.
     * @param qryId Node query id.
     */
    private SqlFieldsQuery createKillQuery(UUID nodeId, long qryId, boolean async) {
        return new SqlFieldsQuery("KILL QUERY" + (async ? " ASYNC" : "") + " '" + nodeId + "_" + qryId + "'");
    }
}

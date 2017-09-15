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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests for partitioned cache queries.
 */
public class IgniteCachePartitionedQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setCommunicationSpi(new TestTcpCommunicationSpi());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        Ignite ignite0 = grid(0);

        IgniteCache<UUID, Person> cache0 = ignite0.cache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.localSize(ALL));

        // Fields query
        QueryCursor<List<?>> qry = cache0
            .query(new SqlFieldsQuery("select name from Person where salary > ?").setArgs(1600));

        Collection<List<?>> res = qry.getAll();

        assertEquals(3, res.size());

        // Fields query count(*)
        qry = cache0.query(new SqlFieldsQuery("select count(*) from Person"));

        res = qry.getAll();

        int cnt = 0;

        for (List<?> row : res)
            cnt += (Long)row.get(0);

        assertEquals(4, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        IgniteCache<UUID, Person> cache0 = grid(0).cache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.localSize(ALL));

        assert grid(0).cluster().nodes().size() == gridCount();

        QueryCursor<Cache.Entry<UUID, Person>> qry =
            cache0.query(new SqlQuery<UUID, Person>(Person.class, "salary < 2000"));

        // Execute on full projection, duplicates are expected.
        Collection<Cache.Entry<UUID, Person>> entries = qry.getAll();

        assert entries != null;

        info("Queried entries: " + entries);

        // Expect result including backup persons.
        assertEquals(gridCount(), entries.size());

        checkResult(entries, p1, p3, p4);
    }

    /**
     * @param entries Queried result.
     * @param persons Persons that should be in the result.
     */
    private void checkResult(Iterable<Cache.Entry<UUID, Person>> entries, Person... persons) {
        for (Cache.Entry<UUID, Person> entry : entries) {
            assertEquals(entry.getKey(), entry.getValue().id());

            assert F.asList(persons).contains(entry.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryPagination() throws Exception {
        final int pageSize = 5;

        final AtomicInteger pages = new AtomicInteger(0);

        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        for (int i = 0; i < 50; i++)
            cache.put(i, i);

        CommunicationSpi spi = ignite().configuration().getCommunicationSpi();

        assert spi instanceof TestTcpCommunicationSpi;

        TestTcpCommunicationSpi commSpi = (TestTcpCommunicationSpi)spi;

        commSpi.filter = new IgniteInClosure<Message>() {
            @Override public void apply(Message msg) {
                if (!(msg instanceof GridIoMessage))
                    return;

                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridCacheQueryRequest) {
                    assertEquals(pageSize, ((GridCacheQueryRequest)msg0).pageSize());

                    pages.incrementAndGet();
                }
                else if (msg0 instanceof GridCacheQueryResponse)
                    assertTrue(((GridCacheQueryResponse)msg0).data().size() <= pageSize);
            }
        };

        try {
            ScanQuery<Integer, Integer> qry = new ScanQuery<Integer, Integer>();

            qry.setPageSize(pageSize);

            List<Cache.Entry<Integer, Integer>> all = cache.query(qry).getAll();

            assertTrue(pages.get() > ignite().cluster().forDataNodes(null).nodes().size());

            assertEquals(50, all.size());
        }
        finally {
            commSpi.filter = null;
        }
    }

    /**
     *
     */
    private static class TestTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        volatile IgniteInClosure<Message> filter;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (filter != null)
                filter.apply(msg);

            super.sendMessage(node, msg, ackC);
        }
    }
}
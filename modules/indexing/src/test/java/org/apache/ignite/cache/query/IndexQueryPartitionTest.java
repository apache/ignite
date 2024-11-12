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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class IndexQueryPartitionTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public boolean client;

    /** */
    private static Map<Integer, Person> data;

    /** */
    @Parameterized.Parameters(name = "mode={0}, client={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[]{ CacheMode.PARTITIONED, false },
            new Object[]{ CacheMode.PARTITIONED, true },
            new Object[]{ CacheMode.REPLICATED, false },
            new Object[]{ CacheMode.REPLICATED, true }
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>()
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(cacheMode)
            .setIndexedTypes(Integer.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(3);

        startClientGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSinglePartition() {
        load();

        for (int part = 0; part < 100; part++) {
            Map<Integer, Person> expRes = expect(part);

            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setPartition(part);

            TestRecordingCommunicationSpi.spi(grid()).record(GridCacheQueryRequest.class);

            QueryCursor<Cache.Entry<Integer, Person>> cursor = grid().cache("CACHE").query(qry);

            for (Cache.Entry<Integer, Person> e: cursor) {
                Person p = expRes.remove(e.getKey());

                assertEquals(e.getKey().toString(), p, e.getValue());
            }

            assertTrue(expRes.isEmpty());

            // Send request to single node only.
            int sendReq = 1;

            if (!client) {
                if (cacheMode == CacheMode.REPLICATED)
                    sendReq = 0;
                else {
                    ClusterNode primNode = grid().affinity("CACHE").mapPartitionToNode(part);

                    if (grid().localNode().equals(primNode))
                        sendReq = 0;
                }
            }

            assertEquals(sendReq, TestRecordingCommunicationSpi.spi(grid()).recordedMessages(true).size());
        }
    }

    /** */
    @Test
    public void testSetNullNotAffect() {
        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid().dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++)
                dataStreamer.addData(i, new Person(rnd.nextInt()));
        }

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(0);

        assertTrue(grid().cache("CACHE").query(qry).getAll().size() < 10_000);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(null);

        assertTrue(grid().cache("CACHE").query(qry).getAll().size() == 10_000);
    }

    /** */
    @Test
    public void testLocalWithPartition() {
        load();

        for (int part = 0; part < 100; part++) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setPartition(part);

            qry.setLocal(true);

            boolean fail = client || (
                    cacheMode == CacheMode.PARTITIONED
                    && !grid().affinity("CACHE").mapPartitionToNode(part).equals(grid().localNode())
                );

            if (fail) {
                GridTestUtils.assertThrows(null, () -> grid().cache("CACHE").query(qry).getAll(),
                    client ? IgniteException.class : CacheInvalidStateException.class,
                    client ? "Failed to execute local index query on a client node." :
                        "Failed to execute index query because required partition has not been found on local node");
            }
            else
                assertTrue(!grid().cache("CACHE").query(qry).getAll().isEmpty());
        }
    }

    /** */
    @Test
    public void testNegativePartitionFails() {
        GridTestUtils.assertThrows(null, () -> new IndexQuery<Integer, Person>(Person.class).setPartition(-1),
            IllegalArgumentException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");

        GridTestUtils.assertThrows(null, () -> new IndexQuery<Integer, Person>(Person.class).setPartition(-23),
            IllegalArgumentException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");

        GridTestUtils.assertThrows(null, () -> {
            IndexQuery qry = new IndexQuery<Integer, Person>(Person.class).setPartition(1000);

            grid().cache("CACHE").query(qry);
        },
            IgniteException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx grid() {
        IgniteEx grid = client ? grid(3) : grid(0);

        assert (client && grid(0).localNode().isClient()) || !grid(0).localNode().isClient();

        return grid;
    }

    /** */
    private void load() {
        data = new HashMap<>();

        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid(0).dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++) {
                Person p = new Person(rnd.nextInt());

                data.put(i, p);
                dataStreamer.addData(i, p);
            }
        }
    }

    /** */
    private Map<Integer, Person> expect(int part) {
        Map<Integer, Person> exp = new HashMap<>();

        for (Integer key: data.keySet()) {
            int p = grid(0).affinity("CACHE").partition(key);

            if (p == part)
                exp.put(key, data.get(key));
        }

        return exp;
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true)
        private final int fld;

        /** */
        Person(int fld) {
            this.fld = fld;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return fld == ((Person)o).fld;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return fld;
        }
    }
}

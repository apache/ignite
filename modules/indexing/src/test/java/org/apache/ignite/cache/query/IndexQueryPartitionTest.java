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
import java.util.Map;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexQueryPartitionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>()
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSinglePartition() {
        int askPart = 23;

        Map<Integer, Person> expRes = new HashMap<>();

        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid(0).dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++) {
                Person p = new Person(rnd.nextInt());

                int part = grid(0).affinity("CACHE").partition(i);

                if (part == askPart)
                    expRes.put(i, p);

                dataStreamer.addData(i, p);
            }
        }

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(askPart);

        TestRecordingCommunicationSpi.spi(grid(0)).record(GridCacheQueryRequest.class);

        QueryCursor<Cache.Entry<Integer, Person>> cursor = grid(0).cache("CACHE").query(qry);

        for (Cache.Entry<Integer, Person> e : cursor) {
            Person p = expRes.remove(e.getKey());

            assertEquals(p, e.getValue());
        }

        assertTrue(expRes.isEmpty());

        // Send request to single node only.
        assertEquals(1, TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true).size());
    }

    /** */
    @Test
    public void testSetNullNotAffect() {
        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid(0).dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++)
                dataStreamer.addData(i, new Person(rnd.nextInt()));
        }

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(0);

        assertTrue(grid(0).cache("CACHE").query(qry).getAll().size() < 10_000);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(null);

        assertTrue(grid(0).cache("CACHE").query(qry).getAll().size() == 10_000);
    }

    /** */
    @Test
    public void testLocalWithPartition() {
        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid(0).dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++)
                dataStreamer.addData(i, new Person(rnd.nextInt()));
        }

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(0);

        qry.setLocal(true);

        ClusterNode node = grid(0).affinity("CACHE").mapPartitionToNode(0);

        for (Ignite ign: G.allGrids()) {
            if (node.equals(((IgniteEx)ign).localNode()))
                assertTrue(!ign.cache("CACHE").query(qry).getAll().isEmpty());
            else {
                GridTestUtils.assertThrows(null, () -> ign.cache("CACHE").query(qry).getAll(),
                    IgniteException.class,
                    "Cluster group is empty.");
            }
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

            grid(0).cache("CACHE").query(qry);
        },
            IgniteException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");
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

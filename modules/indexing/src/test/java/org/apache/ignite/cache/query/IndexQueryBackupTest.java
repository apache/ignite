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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;

/**
 * IndexQuery backup partitions test.
 *
 * Target behavior:
 * local index query (setLocal(true)) must use both primary and backup partitions on the node.
 */
public class IndexQueryBackupTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE = "CACHE";

    /** Entries count. */
    private static final int CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(2)
            .setIndexedTypes(Integer.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));

        cfg.setCacheConfiguration(ccfg);

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

    private boolean contains(int part, int[] parts) {
        for (int i = 0; i < parts.length; i++)
            if (parts[i] == part)
                return true;
        return false;
    }

    /**
     * Checks that local IndexQuery uses both primary and backup partitions on the node.
     */
    @Test
    public void testLocalIndexQueryUsesPrimaryAndBackupPartitions() {
        IgniteEx node = grid(0);

        insertData();

        IgniteCache<Integer, Person> cache = node.cache(CACHE);

        Affinity<Object> affinity = node.affinity(CACHE);

        int[] primaryPartitions = affinity.primaryPartitions(node.localNode());

        int[] backupPartitions = affinity.backupPartitions(node.localNode());

        int uniqPart = -1;

        for (int part : backupPartitions) {
            if (contains(part, primaryPartitions))
                continue;
            uniqPart = part;
            break;
        }

        System.out.println(primaryPartitions.length + " " + backupPartitions.length);
        System.out.println(Arrays.toString(primaryPartitions));
        System.out.println(Arrays.toString(backupPartitions));
        System.out.println("Uniq part = " + uniqPart);

        int from = 0;
        int to = CNT - 1;

        Set<Integer> expKeys = expectedLocalKeys(cache, from, to);

        IgniteCache<Integer, BinaryObject> binCache = cache.withKeepBinary();

        IndexQuery<Integer, BinaryObject> qry = (IndexQuery<Integer, BinaryObject>)new IndexQuery<Integer, BinaryObject>(Person.class)
            .setCriteria(between("fld", from, to))
//            .includeBackups(true)
//            .setPartition(uniqPart)
            .setLocal(true);

        QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = binCache.query(qry);

        Set<Integer> actualKeys = new HashSet<>();

        for (Cache.Entry<Integer, BinaryObject> e : cursor) {
            boolean added = actualKeys.add(e.getKey());
            System.out.println(e.getKey() + " " + added);

            assertTrue("Duplicate key in query result: " + e.getKey(), added);
        }

        System.out.println(expKeys.size() + " " + actualKeys.size());

        assertEquals(expKeys, actualKeys);
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Integer, Person> streamer = grid(0).dataStreamer(CACHE)) {
            for (int i = 0; i < CNT; i++)
                streamer.addData(i, new Person(i));
        }
    }

    /** */
    private Set<Integer> expectedLocalKeys(IgniteCache<Integer, Person> cache, int from, int to) {
        Set<Integer> res = new HashSet<>();

        for (Cache.Entry<Integer, Person> e :
            cache.localEntries(CachePeekMode.PRIMARY, CachePeekMode.BACKUP)) {
            Integer key = e.getKey();

            if (key >= from && key <= to)
                res.add(key);
        }

        return res;
    }

    /**
     * Simple value type with indexed field.
     */
    private static class Person {
        /** Indexed field. */
        @GridToStringInclude
        @QuerySqlField(index = true)
        private final int fld;

        /** */
        Person(int fld) {
            this.fld = fld;
        }

        /** */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return fld == person.fld;
        }

        /** */
        @Override public int hashCode() {
            return fld;
        }
    }
}

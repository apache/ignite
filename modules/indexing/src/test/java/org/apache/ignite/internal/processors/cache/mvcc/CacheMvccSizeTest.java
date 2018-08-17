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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.cache.CachePeekMode.BACKUP;

/**
 *
 */
public class CacheMvccSizeTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsert() throws Exception {
        IgniteEx ignite = startGrid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);

        personTbl.query(q("begin"));
        personTbl.query(q("insert into person values(1, 'a')"));

        assertEquals(0, personTbl.size());
        personTbl.query(q("commit"));

        assertEquals(1, personTbl.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelete() throws Exception {
        IgniteEx ignite = startGrid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);
        personTbl.query(q("insert into person values(1, 'a')"));

        personTbl.query(q("begin"));
        personTbl.query(q("delete from person"));

        assertEquals(1, personTbl.size());
        personTbl.query(q("commit"));

        assertEquals(0, personTbl.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertUpdateConcurrent() throws Exception {
        IgniteEx ignite = startGrid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);

        CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> {
                for (int i = 0; i < 100; i++)
                    personTbl.query(q("insert into person values(%d, 'a')", i));
            }),
            CompletableFuture.runAsync(() -> {
                Random random = new Random();
                for (int i = 0; i < 1000; i++)
                    personTbl.query(q("update person set name = 'b' where id = %d", random.nextInt(100)));
            })
        ).join();

        assertEquals(100, personTbl.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertMultipleKeys() throws Exception {
        IgniteEx ignite = startGrid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);

        personTbl.query(q("begin"));
        personTbl.query(q("insert into person values(1, 'a')"));
        personTbl.query(q("insert into person values(%d, 'b')", keyInSamePartition(ignite, 1)));
        personTbl.query(q("insert into person values(%d, 'c')", keyInDifferentPartition(ignite, 1)));

        assertEquals(0, personTbl.size());
        personTbl.query(q("commit"));

        assertEquals(3, personTbl.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertWithBackups() throws Exception {
        startGridsMultiThreaded(2);
        IgniteEx ignite = grid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);

        personTbl.query(q("begin"));
        personTbl.query(q("insert into person values(1, 'a')"));

        assertEquals(0, personTbl.size(BACKUP));
        personTbl.query(q("commit"));

        assertEquals(1, personTbl.size(BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteWithBackups() throws Exception {
        startGridsMultiThreaded(2);
        IgniteEx ignite = grid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);
        personTbl.query(q("insert into person values(1, 'a')"));

        personTbl.query(q("begin"));
        personTbl.query(q("delete from person where id = 1"));

        assertEquals(1, personTbl.size(BACKUP));
        personTbl.query(q("commit"));

        assertEquals(0, personTbl.size(BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertAndDeleteWithBackups() throws Exception {
        startGridsMultiThreaded(2);
        IgniteEx ignite = grid(0);
        IgniteCache<?, ?> personTbl = createTable(ignite);
        personTbl.query(q("insert into person values(1, 'a')"));

        personTbl.query(q("begin"));
        personTbl.query(q("insert into person values(2, 'b')"));
        personTbl.query(q("delete from person where id = 1"));

        assertEquals(1, personTbl.size());
        assertEquals(1, personTbl.size(BACKUP));
        personTbl.query(q("commit"));

        assertEquals(1, personTbl.size());
        assertEquals(1, personTbl.size(BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitialValueWithBackups() throws Exception {
        startGridsMultiThreaded(2);
        IgniteEx ignite = grid(0);
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>("test")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.REPLICATED)
        );

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer("test")) {
            streamer.addData(1, "a");
            streamer.flush();
        }

        assertEquals(1, cache.size());
        assertEquals(1, cache.size(BACKUP));
    }

    /** */
    private static IgniteCache<?, ?> createTable(IgniteEx ignite) {
        IgniteCache<?, ?> sqlNexus = ignite.getOrCreateCache(new CacheConfiguration<>("sqlNexus").setSqlSchema("PUBLIC"));
        sqlNexus.query(q("" +
            "create table person(" +
            "  id int primary key," +
            "  name varchar" +
            ") with \"atomicity=transactional,template=replicated,cache_name=person\""));
        assert ignite.cachex("person").configuration().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL;
        assert ignite.cachex("person").configuration().getCacheMode() == CacheMode.REPLICATED;
        return ignite.cache("person");
    }

    /** */
    private static SqlFieldsQuery q(String fSql, Object... args) {
        return new SqlFieldsQuery(String.format(fSql, args));
    }

    /** */
    private static int keyInSamePartition(Ignite ignite, int key) {
        Affinity<Object> affinity = ignite.affinity("person");
        return IntStream.iterate(key + 1, i -> i + 1)
            .filter(i -> affinity.partition(i) == affinity.partition(key))
            .findFirst().getAsInt();
    }

    /** */
    private static int keyInDifferentPartition(Ignite ignite, int key) {
        Affinity<Object> affinity = ignite.affinity("person");
        return IntStream.iterate(key + 1, i -> i + 1)
            .filter(i -> affinity.partition(i) != affinity.partition(key))
            .findFirst().getAsInt();
    }
}

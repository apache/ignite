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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
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
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

import static org.apache.ignite.cache.CachePeekMode.BACKUP;

/**
 *
 */
public class CacheMvccSizeTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** */
    private void checkSizeModificationByOperation(String sql, boolean commit, int expSizeDelta) throws Exception {
        checkSizeModificationByOperation(c -> {}, cache -> cache.query(q(sql)).getAll(), commit, expSizeDelta);
    }

    /** */
    private void checkSizeModificationByOperation(String initSql, String sql, boolean commit,
        int expSizeDelta) throws Exception {
        checkSizeModificationByOperation(
            cache -> cache.query(q(initSql)).getAll(),
            cache -> cache.query(q(sql)).getAll(),
            commit,
            expSizeDelta);
    }

    /** */
    private void checkSizeModificationByOperation(Consumer<IgniteCache<?, ?>> inTx, boolean commit,
        int expSizeDelta) throws Exception {
        checkSizeModificationByOperation(c -> {}, inTx, commit, expSizeDelta);
    }

    /** */
    private void checkSizeModificationByOperation(Consumer<IgniteCache<?, ?>> beforeTx,
        Consumer<IgniteCache<?, ?>> inTx, boolean commit, int expSizeDelta) throws Exception {
        IgniteCache<Object, Object> tbl0 = grid(0).cache("person");

        tbl0.query(q("delete from person"));

        beforeTx.accept(tbl0);

        int initSize = tbl0.size();

        tbl0.query(q("begin"));

        inTx.accept(tbl0);

        // size is not changed before commit
        assertEquals(0, tbl0.size() - initSize);

        if (commit)
            tbl0.query(q("commit"));
        else
            tbl0.query(q("rollback"));

        assertEquals(expSizeDelta, tbl0.size() - initSize);
        assertEquals(tbl0.size(), table(grid(1)).size());

        assertEquals(tbl0.size(), tbl0.size(BACKUP));
        assertEquals(tbl0.size(), table(grid(1)).size(BACKUP));
    }

    /**
     * @throws Exception if failed.
     */
    public void testSql() throws Exception {
        startGridsMultiThreaded(2);

        createTable(grid(0));

        checkSizeModificationByOperation("insert into person values(1, 'a')", true, 1);

        checkSizeModificationByOperation("insert into person values(1, 'a')", false, 0);

        checkSizeModificationByOperation(
            personTbl -> personTbl.query(q("insert into person values(1, 'a')")),
            personTbl -> {
                try {
                    personTbl.query(q("insert into person values(1, 'a')"));
                }
                catch (Exception e) {
                    if (e.getCause() instanceof IgniteSQLException) {
                        assertEquals(IgniteQueryErrorCode.DUPLICATE_KEY,
                            ((IgniteSQLException)e.getCause()).statusCode());
                    }
                    else {
                        e.printStackTrace();

                        fail("Unexpected exceptions");
                    }
                }
            },
            true, 0);

        checkSizeModificationByOperation("merge into person(id, name) values(1, 'a')", true, 1);

        checkSizeModificationByOperation("merge into person(id, name) values(1, 'a')", false, 0);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "merge into person(id, name) values(1, 'b')", true, 0);

        checkSizeModificationByOperation("update person set name = 'b' where id = 1", true, 0);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "update person set name = 'b' where id = 1", true, 0);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "delete from person where id = 1", true, -1);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "delete from person where id = 1", false, 0);

        checkSizeModificationByOperation("delete from person where id = 1", true, 0);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "select * from person", true, 0);

        checkSizeModificationByOperation("select * from person", true, 0);

        checkSizeModificationByOperation(
            "insert into person values(1, 'a')", "select * from person where id = 1 for update", true, 0);

        checkSizeModificationByOperation("select * from person where id = 1 for update", true, 0);

        checkSizeModificationByOperation(personTbl -> {
            personTbl.query(q("insert into person values(1, 'a')"));

            personTbl.query(q("insert into person values(%d, 'b')", keyInSamePartition(grid(0), "person", 1)));

            personTbl.query(q("insert into person values(%d, 'c')", keyInDifferentPartition(grid(0), "person", 1)));
        }, true, 3);

        checkSizeModificationByOperation(personTbl -> {
            personTbl.query(q("insert into person values(1, 'a')"));

            personTbl.query(q("delete from person where id = 1"));
        }, true, 0);

        checkSizeModificationByOperation(personTbl -> {
            personTbl.query(q("insert into person values(1, 'a')"));

            personTbl.query(q("delete from person where id = 1"));

            personTbl.query(q("insert into person values(1, 'a')"));
        }, true, 1);

        checkSizeModificationByOperation(
            personTbl -> personTbl.query(q("insert into person values(1, 'a')")),
            personTbl -> {
                personTbl.query(q("delete from person where id = 1"));

                personTbl.query(q("insert into person values(1, 'a')"));
            }, true, 0);

        checkSizeModificationByOperation(personTbl -> {
            personTbl.query(q("merge into person(id, name) values(1, 'a')"));

            personTbl.query(q("delete from person where id = 1"));
        }, true, 0);

        checkSizeModificationByOperation(personTbl -> {
            personTbl.query(q("merge into person(id, name) values(1, 'a')"));

            personTbl.query(q("delete from person where id = 1"));

            personTbl.query(q("merge into person(id, name) values(1, 'a')"));
        }, true, 1);

        checkSizeModificationByOperation(
            personTbl -> personTbl.query(q("merge into person(id, name) values(1, 'a')")),
            personTbl -> {
                personTbl.query(q("delete from person where id = 1"));

                personTbl.query(q("merge into person(id, name) values(1, 'a')"));
            }, true, 0);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteConcurrent() throws Exception {
        startGridsMultiThreaded(2);

        IgniteCache<?, ?> tbl0 = createTable(grid(0));

        SqlFieldsQuery insert = new SqlFieldsQuery("insert into person(id, name) values(?, 'a')");

        SqlFieldsQuery delete = new SqlFieldsQuery("delete from person where id = ?");

        CompletableFuture<Integer> insertFut = CompletableFuture.supplyAsync(() -> {
            int cnt = 0;

            for (int i = 0; i < 1000; i++)
                cnt += update(insert.setArgs(ThreadLocalRandom.current().nextInt(10)), tbl0);

            return cnt;
        });

        CompletableFuture<Integer> deleteFut = CompletableFuture.supplyAsync(() -> {
            int cnt = 0;

            for (int i = 0; i < 1000; i++)
                cnt += update(delete.setArgs(ThreadLocalRandom.current().nextInt(10)), tbl0);

            return cnt;
        });

        int expSize = insertFut.join() - deleteFut.join();

        assertEquals(expSize, tbl0.size());
        assertEquals(expSize, table(grid(1)).size());

        assertEquals(expSize, tbl0.size(BACKUP));
        assertEquals(expSize, table(grid(1)).size(BACKUP));
    }

    /** */
    private int update(SqlFieldsQuery qry, IgniteCache<?, ?> cache) {
        try {
            return Integer.parseInt(cache.query(qry).getAll().get(0).get(0).toString());
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWriteConflictDoesNotChangeSize() throws Exception {
        startGridsMultiThreaded(2);

        IgniteCache<?, ?> tbl0 = createTable(grid(0));

        tbl0.query(q("insert into person values(1, 'a')"));

        tbl0.query(q("begin"));

        tbl0.query(q("delete from person where id = 1"));

        CompletableFuture<Void> conflictingStarted = new CompletableFuture<>();

        CompletableFuture<Void> fut = CompletableFuture.runAsync(() -> {
            tbl0.query(q("begin"));

            try {
                tbl0.query(q("select * from person")).getAll();
                conflictingStarted.complete(null);

                tbl0.query(q("merge into person(id, name) values(1, 'b')"));
            }
            finally {
                tbl0.query(q("commit"));
            }
        });

        conflictingStarted.join();
        tbl0.query(q("commit"));

        try {
            fut.join();
        }
        catch (Exception e) {
            if (e.getCause().getCause() instanceof IgniteSQLException)
                assertTrue(e.getMessage().toLowerCase().contains("version mismatch"));
            else {
                e.printStackTrace();

                fail("Unexpected exception");
            }
        }

        assertEquals(0, tbl0.size());
        assertEquals(0, table(grid(1)).size());

        assertEquals(0, tbl0.size(BACKUP));
        assertEquals(0, table(grid(1)).size(BACKUP));
    }

    /**
     * @throws Exception if failed.
     */
    public void testDeleteChangesSizeAfterUnlock() throws Exception {
        startGridsMultiThreaded(2);

        IgniteCache<?, ?> tbl0 = createTable(grid(0));

        tbl0.query(q("insert into person values(1, 'a')"));

        tbl0.query(q("begin"));

        tbl0.query(q("select * from person where id = 1 for update")).getAll();

        CompletableFuture<Thread> asyncThread = new CompletableFuture<>();

        CompletableFuture<Void> fut = CompletableFuture.runAsync(() -> {
            tbl0.query(q("begin"));

            try {
                tbl0.query(q("select * from person")).getAll();

                asyncThread.complete(Thread.currentThread());
                tbl0.query(q("delete from person where id = 1"));
            }
            finally {
                tbl0.query(q("commit"));
            }
        });

        Thread concThread = asyncThread.join();

        // wait until concurrent thread blocks awaiting entry mvcc lock release
        while (concThread.getState() == Thread.State.RUNNABLE && !Thread.currentThread().isInterrupted());

        tbl0.query(q("commit"));

        fut.join();

        assertEquals(0, tbl0.size());
        assertEquals(0, table(grid(1)).size());

        assertEquals(0, tbl0.size(BACKUP));
        assertEquals(0, table(grid(1)).size(BACKUP));
    }

    /**
     * @throws Exception if failed.
     */
    public void testDataStreamerModifiesReplicatedCacheSize() throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx ignite = grid(0);

        ignite.createCache(
            new CacheConfiguration<>("test")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.REPLICATED)
        );

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer("test")) {
            streamer.addData(1, "a");

            streamer.addData(keyInDifferentPartition(ignite, "test", 1), "b");
        }

        assertEquals(2, ignite.cache("test").size());

        assertEquals(1, grid(0).cache("test").localSize());
        assertEquals(1, grid(0).cache("test").localSize(BACKUP));

        assertEquals(1, grid(1).cache("test").localSize());
        assertEquals(1, grid(1).cache("test").localSize(BACKUP));
    }

    /**
     * @throws Exception if failed.
     */
    public void testSizeIsConsistentAfterRebalance() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<?, ?> tbl = createTable(ignite);

        for (int i = 0; i < 100; i++)
            tbl.query(q("insert into person values(?, ?)").setArgs(i, i));

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<?, ?> tbl0 = grid(0).cache("person");
        IgniteCache<?, ?> tbl1 = grid(1).cache("person");

        assert tbl0.localSize() != 0 && tbl1.localSize() != 0;

        assertEquals(100, tbl1.size());
        assertEquals(100, tbl0.localSize() + tbl1.localSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSizeIsConsistentAfterRebalanceDuringInsert() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<?, ?> tbl = createTable(ignite);

        Future<?> f = null;

        for (int i = 0; i < 100; i++) {
            if (i == 50)
                f = ForkJoinPool.commonPool().submit(() -> startGrid(1));

            tbl.query(q("insert into person values(?, ?)").setArgs(i, i));
        }

        f.get();

        awaitPartitionMapExchange();

        IgniteCache<?, ?> tbl0 = grid(0).cache("person");
        IgniteCache<?, ?> tbl1 = grid(1).cache("person");

        assert tbl0.localSize() != 0 && tbl1.localSize() != 0;

        assertEquals(100, tbl1.size());
        assertEquals(100, tbl0.localSize() + tbl1.localSize());
    }

    /** */
    private static IgniteCache<?, ?> table(IgniteEx ignite) {
        assert ignite.cachex("person").configuration().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
        assert ignite.cachex("person").configuration().getCacheMode() == CacheMode.REPLICATED;

        return ignite.cache("person");
    }

    /** */
    private static IgniteCache<?, ?> createTable(IgniteEx ignite) {
        IgniteCache<?, ?> sqlNexus = ignite.getOrCreateCache(new CacheConfiguration<>("sqlNexus").setSqlSchema("PUBLIC"));

        sqlNexus.query(q("" +
            "create table person(" +
            "  id int primary key," +
            "  name varchar" +
            ") with \"atomicity=transactional_snapshot,template=replicated,cache_name=person\""));

        return table(ignite);
    }

    /** */
    private static SqlFieldsQuery q(String fSql, Object... args) {
        return new SqlFieldsQuery(String.format(fSql, args));
    }

    /** */
    private static int keyInSamePartition(Ignite ignite, String cacheName, int key) {
        Affinity<Object> affinity = ignite.affinity(cacheName);

        return IntStream.iterate(key + 1, i -> i + 1)
            .filter(i -> affinity.partition(i) == affinity.partition(key))
            .findFirst().getAsInt();
    }

    /** */
    private static int keyInDifferentPartition(Ignite ignite, String cacheName, int key) {
        Affinity<Object> affinity = ignite.affinity(cacheName);

        return IntStream.iterate(key + 1, i -> i + 1)
            .filter(i -> affinity.partition(i) != affinity.partition(key))
            .findFirst().getAsInt();
    }
}

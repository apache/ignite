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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.profiling.IgniteProfiling.CacheOperationType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.profiling.LogFileProfiling.PROFILING_DIR;

/**
 * Tests profiling.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class ProfilingSelfTest extends GridCommonAbstractTest {
    /** Test entry processor. */
    private static final EntryProcessor<Object, Object, Object> ENTRY_PROC =
        new EntryProcessor<Object, Object, Object>() {
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            return null;
        }
    };

    /** Test cache entry processor. */
    private static final CacheEntryProcessor<Object, Object, Object> CACHE_ENTRY_PROC =
        new CacheEntryProcessor<Object, Object, Object>() {
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            return null;
        }
    };

    /** Log to register profiling operations. */
    private static ListeningTestLogger log;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        log = new ListeningTestLogger(GridAbstractTest.log);

        cleanPersistenceDir();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        new TestProfilingLogReader(grid(0), log).startRead();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), PROFILING_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        log.clearListeners();
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        checkSqlFieldsQuery("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL VARCHAR)");

        checkSqlFieldsQuery("INSERT INTO TEST VALUES (?, ?)", 1, "1");
        checkSqlFieldsQuery("INSERT INTO TEST VALUES (?, ?)", 2, "2");

        checkSqlFieldsQuery("UPDATE TEST SET VAL='2' WHERE ID = ?", 1);

        checkSqlFieldsQuery("SELECT * FROM TEST");

        checkSqlFieldsQuery("DROP TABLE TEST");
    }

    /** */
    private void checkSqlFieldsQuery(String sql, Object... args) throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        LogListener lsnr = LogListener.matches("query ").andMatches(sql).andMatches("type=SQL_FIELDS").build();

        log.registerListener(lsnr);

        grid(0).context().query().querySqlFields(qry, true).getAll();

        assertTrue(lsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testScanQuery() throws Exception {
        LogListener lsnr = LogListener.matches("query ").andMatches("type=SCAN").build();

        log.registerListener(lsnr);

        grid(0).cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();

        assertTrue(lsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        LogListener taskLsnr = LogListener.matches("task ").build();

        LogListener jobLsnr = LogListener.matches("job ").build();

        log.registerListener(jobLsnr);
        log.registerListener(taskLsnr);

        grid(0).compute().broadcast(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                U.sleep(10);

                return null;
            }
        });

        assertTrue(taskLsnr.check(30_000));
        assertTrue(jobLsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOps() throws Exception {
        checkCacheOp(CacheOperationType.PUT, cache -> cache.put(1, 1));
        checkCacheOp(CacheOperationType.PUT, cache -> cache.putAsync(2, 2).get());

        checkCacheOp(CacheOperationType.PUT_ALL, cache -> cache.putAll(Collections.singletonMap(3, 3)));
        checkCacheOp(CacheOperationType.PUT_ALL, cache -> cache.putAllAsync(Collections.singletonMap(4, 4)).get());

        checkCacheOp(CacheOperationType.GET, cache -> cache.get(1));
        checkCacheOp(CacheOperationType.GET, cache -> cache.getAsync(2).get());

        checkCacheOp(CacheOperationType.GET_ALL, cache -> cache.getAll(Collections.singleton(1)));
        checkCacheOp(CacheOperationType.GET_ALL, cache -> cache.getAllAsync(Collections.singleton(2)).get());

        checkCacheOp(CacheOperationType.REMOVE, cache -> cache.remove(1));
        checkCacheOp(CacheOperationType.REMOVE, cache -> cache.removeAsync(2).get());

        checkCacheOp(CacheOperationType.REMOVE_ALL, cache -> cache.removeAll(Collections.singleton(3)));
        checkCacheOp(CacheOperationType.REMOVE_ALL, cache -> cache.removeAllAsync(Collections.singleton(4)).get());

        checkCacheOp(CacheOperationType.LOCK, cache -> {
            Lock lock = cache.lock(5);

            lock.lock();
            lock.unlock();
        });

        checkCacheOp(CacheOperationType.LOCK, cache -> {
            Lock lock = cache.lockAll(Collections.singleton(5));

            lock.lock();
            lock.unlock();
        });

        checkCacheOp(CacheOperationType.INVOKE, cache -> cache.invoke(10, ENTRY_PROC));
        checkCacheOp(CacheOperationType.INVOKE, cache -> cache.invokeAsync(10, ENTRY_PROC).get());

        checkCacheOp(CacheOperationType.INVOKE, cache -> cache.invoke(10, CACHE_ENTRY_PROC));
        checkCacheOp(CacheOperationType.INVOKE, cache -> cache.invokeAsync(10, CACHE_ENTRY_PROC).get());

        checkCacheOp(CacheOperationType.INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), ENTRY_PROC));
        checkCacheOp(CacheOperationType.INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), ENTRY_PROC).get());

        checkCacheOp(CacheOperationType.INVOKE_ALL,
            cache -> cache.invokeAll(Collections.singleton(10), CACHE_ENTRY_PROC));
        checkCacheOp(CacheOperationType.INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), CACHE_ENTRY_PROC).get());
    }

    /** */
    private void checkCacheOp(CacheOperationType op, Consumer<IgniteCache<Object, Object>> clo) throws Exception {
        LogListener lsnr = LogListener.matches("cacheOperation ").andMatches("type=" + op).build();

        log.registerListener(lsnr);

        clo.accept(grid(0).cache(DEFAULT_CACHE_NAME));

        assertTrue(lsnr.check(30_000));

        log.clearListeners();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCommit() throws Exception {
        checkTx(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRollback() throws Exception {
        checkTx(false);
    }

    /** @param commit {@code True} if check transaction commit. */
    private void checkTx(boolean commit) throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        LogListener lsnr = LogListener.matches("transaction ").andMatches("commit=" + commit).build();

        log.registerListener(lsnr);

        try (Transaction tx = grid(0).transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                cache.put(i, i * 2);

            if (commit)
                tx.commit();
            else
                tx.rollback();
        }

        assertTrue(lsnr.check(30_000));
    }
}

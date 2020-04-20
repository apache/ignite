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
import java.util.regex.Pattern;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.regex.Pattern.compile;

/**
 * Tests profiling log.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class ProfilingLogTest extends GridCommonAbstractTest {
    /** */
    private static final Pattern CACHE_OPS_PATTERN = compile(
        "cache \\[op=(get|getAll|put|putAll|remove|removeAll|getAndPut|getAndRemove|lock|invoke|invokeAll)," +
            " cacheId=-?\\d+, startTime=\\d+, duration=\\d+]$");

    /** */
    public static final Pattern QUERY_PATTERN = compile(
        "query \\[type=.+, query=.+, id=.+, startTime=\\d+, duration=\\d+, success=(true|false)]$");

    /** */
    public static final Pattern QUERY_READS_PATTERN = compile(
        "queryStat \\[type=.+, id=.+, logicalReads=\\d+, physicalReads=\\d+]$");

    /** */
    private static final Pattern TASK_PATTERN = compile(
        "task \\[sesId=.+, taskName=.*, startTime=\\d+, duration=\\d+, affPartId=.*]$");

    /** */
    private static final Pattern JOB_PATTERN = compile(
        "job \\[sesId=.+, queuedTime=\\d+, startTime=\\d+, duration=\\d+, isTimedOut=(true|false)]$");

    /** */
    private static final Pattern PME_PATTERN = compile(
        "pme \\[duration=\\d+, reason=.+, blocking=(true|false), resVer=.+]$");

    /** */
    private static final Pattern TX_PATTERN = compile(
        "tx \\[cacheIds=.+, startTime=\\d+, duration=\\d+, commit=(true|false)]$");

    /** */
    private static ListeningTestLogger log;

    /** */
    private static final EntryProcessor<Object, Object, Object> ENTRY_PROC =
        new EntryProcessor<Object, Object, Object>() {
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            return null;
        }
    };

    /** */
    private static final CacheEntryProcessor<Object, Object, Object> CACHE_ENTRY_PROC =
        new CacheEntryProcessor<Object, Object, Object>() {
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            return null;
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        cfg.setGridLogger(log);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        log = new ListeningTestLogger(false, GridAbstractTest.log);

        cleanPersistenceDir();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
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

        LogListener lsnr = LogListener.matches(QUERY_PATTERN).andMatches(sql).andMatches("SQL_FIELDS").build();

        log.registerListener(lsnr);

        grid(0).context().query().querySqlFields(qry, true).getAll();

        assertTrue(lsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testScanQuery() throws Exception {
        LogListener lsnr = LogListener.matches(QUERY_PATTERN).andMatches("SCAN").build();

        log.registerListener(lsnr);

        grid(0).cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();

        assertTrue(lsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        LogListener jobLsnr = LogListener.matches(JOB_PATTERN)
            .times(grid(0).cluster().forServers().nodes().size()).build();

        LogListener taskLsnr = LogListener.matches(TASK_PATTERN).times(1).build();

        log.registerListener(jobLsnr);
        log.registerListener(taskLsnr);

        grid(0).compute().broadcast(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                U.sleep(10);

                return null;
            }
        });

        assertTrue(jobLsnr.check(30_000));
        assertTrue(taskLsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testPme() throws Exception {
        LogListener lsnr = LogListener.matches(PME_PATTERN).build();

        log.registerListener(lsnr);

        startGrid(3);
        stopGrid(3);

        assertTrue(lsnr.check(30_000));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOps() throws Exception {
        checkCacheOp("put", cache -> cache.put(1, 1));
        checkCacheOp("put", cache -> cache.putAsync(2, 2).get());

        checkCacheOp("putAll", cache -> cache.putAll(Collections.singletonMap(3, 3)));
        checkCacheOp("putAll", cache -> cache.putAllAsync(Collections.singletonMap(4, 4)).get());

        checkCacheOp("get", cache -> cache.get(1));
        checkCacheOp("get", cache -> cache.getAsync(2).get());

        checkCacheOp("getAll", cache -> cache.getAll(Collections.singleton(1)));
        checkCacheOp("getAll", cache -> cache.getAllAsync(Collections.singleton(2)).get());

        checkCacheOp("remove", cache -> cache.remove(1));
        checkCacheOp("remove", cache -> cache.removeAsync(2).get());

        checkCacheOp("removeAll", cache -> cache.removeAll(Collections.singleton(3)));
        checkCacheOp("removeAll", cache -> cache.removeAllAsync(Collections.singleton(4)).get());

        checkCacheOp("lock", cache -> {
            Lock lock = cache.lock(5);

            lock.lock();
            lock.unlock();
        });

        checkCacheOp("lock", cache -> {
            Lock lock = cache.lockAll(Collections.singleton(5));

            lock.lock();
            lock.unlock();
        });

        checkCacheOp("invoke", cache -> cache.invoke(10, ENTRY_PROC));
        checkCacheOp("invoke", cache -> cache.invokeAsync(10, ENTRY_PROC).get());
        checkCacheOp("invoke", cache -> cache.invoke(10, CACHE_ENTRY_PROC));
        checkCacheOp("invoke", cache -> cache.invokeAsync(10, CACHE_ENTRY_PROC).get());

        checkCacheOp("invokeAll", cache -> cache.invokeAll(Collections.singleton(10), ENTRY_PROC));
        checkCacheOp("invokeAll", cache -> cache.invokeAllAsync(Collections.singleton(10), ENTRY_PROC).get());
        checkCacheOp("invokeAll", cache -> cache.invokeAll(Collections.singleton(10), CACHE_ENTRY_PROC));
        checkCacheOp("invokeAll", cache -> cache.invokeAllAsync(Collections.singleton(10), CACHE_ENTRY_PROC).get());
    }

    /** */
    private void checkCacheOp(String op, Consumer<IgniteCache<Object, Object>> clo) throws Exception {
        LogListener lsnr = LogListener.matches(CACHE_OPS_PATTERN).andMatches(op).build();

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

        LogListener lsnr = LogListener.matches(TX_PATTERN).andMatches("commit=" + commit).build();

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

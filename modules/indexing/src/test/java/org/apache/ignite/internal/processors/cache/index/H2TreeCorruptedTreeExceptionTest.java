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
package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/** */
public class H2TreeCorruptedTreeExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final String IDX_NAME = "A_IDX";

    /** */
    private static final String GRP_NAME = "cacheGrp";

    /** */
    private static final String VERY_SENS_STR_DATA = "here_comes_very_sensitive_data@#123#321#@";

    /** */
    private final AtomicBoolean failWithCorruptTree = new AtomicBoolean(false);

    /** */
    private final LogListener logListener = new MessageOrderLogListener(
        String.format(
                ".*?Tree is corrupted.*?cacheId=-1578586276, cacheName=SQL_PUBLIC_A, indexName=%s, groupName=%s" +
                    ", msg=Runtime failure on row: Row@.*?key: 1, val: .*?%s.*",
                IDX_NAME,
                GRP_NAME,
                IGNITE_TO_STRING_INCLUDE_SENSITIVE
        )
    );

    /** */
    private final LogListener logSensListener = new MessageOrderLogListener(String.format(".*%s.*", VERY_SENS_STR_DATA));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        ListeningTestLogger listeningTestLog = new ListeningTestLogger(false, log);

        listeningTestLog.registerListener(logListener);

        listeningTestLog.registerListener(logSensListener);

        cfg.setGridLogger(listeningTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (hnd instanceof BPlusTree.Insert) {
                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler<Object, BPlusTree.Result>() {
                    @Override public BPlusTree.Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int intArg) throws IgniteCheckedException {
                        BPlusTree.Result res =
                            delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, intArg);

                        if (failWithCorruptTree.get() && tree instanceof H2Tree && tree.getName().contains(IDX_NAME))
                            throw new RuntimeException("test exception");

                        return res;
                    }
                };
            }
            else
                return hnd;
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        clearGridToStringClassCache();

        BPlusTree.testHndWrapper = null;

        super.afterTest();
    }

    /** */
    public void testCorruptedTree() throws Exception {
        withSystemProperty(IGNITE_TO_STRING_INCLUDE_SENSITIVE, "false");

        IgniteEx srv = startGrid(0);

        srv.cluster().active(true);

        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridQueryProcessor qryProc = srv.context().query();

        qryProc.querySqlFields(new SqlFieldsQuery("create table a (col1 varchar primary key, col2 varchar) with \"CACHE_GROUP="
            + GRP_NAME + "\""), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("create index " + IDX_NAME + " on a(col2)"), true).getAll();

        failWithCorruptTree.set(true);

        try {
            qryProc.querySqlFields(new SqlFieldsQuery("insert into a(col1, col2) values (1, ?1)").
                setArgs(VERY_SENS_STR_DATA), true).getAll();

            fail("Cache operations are expected to fail");
        }
        catch (Throwable e) {
            Throwable ex = findCauseCorruptedTreeExceptionIfExists(e, 0);

            assertTrue(ex != null && ex instanceof CorruptedTreeException);
        }

        assertTrue(logListener.check());

        assertFalse(logSensListener.check());

        System.setProperty(IGNITE_TO_STRING_INCLUDE_SENSITIVE, "true");

        clearGridToStringClassCache();

        logListener.reset();

        logSensListener.reset();

        try {
            qryProc.querySqlFields(new SqlFieldsQuery("insert into a(col1, col2) values (2, ?1)").
                setArgs(VERY_SENS_STR_DATA), true).getAll();
        }
        catch (Throwable e) {
            Throwable ex = findCauseCorruptedTreeExceptionIfExists(e, 0);

            assertTrue(ex != null && ex instanceof CorruptedTreeException);
        }

        assertFalse(logListener.check());

        assertTrue(logSensListener.check());
    }

    /**
     * Finds cause CorruptedTreeException between causes and suppressed exceptions of given exception.
     *
     * @param e Exception.
     * @param depth Depth to limit calls.
     * @return Cause CorruptedTreeException.
     */
    private Throwable findCauseCorruptedTreeExceptionIfExists(Throwable e, int depth) {
        if (depth == 10)
            return null; //limit search

        Throwable res;

        Throwable ex = e.getCause();

        if (ex == null || ex instanceof CorruptedTreeException)
            res = ex;
        else
            res = findCauseCorruptedTreeExceptionIfExists(ex, depth + 1);

        if (res != null)
            return res;

        Throwable[] suppressed = e.getSuppressed();

        for (Throwable s : suppressed) {
            if (s instanceof CorruptedTreeException)
                return s;

            res = findCauseCorruptedTreeExceptionIfExists(s, depth + 1);

            if (res instanceof CorruptedTreeException)
                return res;
        }

        return res;
    }
}

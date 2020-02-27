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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.result.SearchRow;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * Tests failed start of iteration through index.
 */
public class GridCommandHandlerBrokenIndexTest extends GridCommandHandlerClusterPerMethodAbstractTest{
    /** */
    private static final String EXCEPTION_MSG = "Exception from BadIndex#find";

    /** */
    private static final String IDX_ISSUE_STR = "IndexValidationIssue \\[key=[0-9]*, cacheName=" + CACHE_NAME +
        ", idxName=null], class java.lang.RuntimeException: " + EXCEPTION_MSG;

    /** */
    private List<LogListener> lsnrs = new ArrayList<>();

    /**
     * Adds error message listeners to server nodes.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode())
            return cfg;

        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        Pattern logErrMsgPattern = Pattern.compile("Failed to lookup key: " + IDX_ISSUE_STR);

        LogListener lsnr = LogListener.matches(logErrMsgPattern).build();

        testLog.registerListener(lsnr);

        lsnrs.add(lsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /**
     * Tests Cursor initialisation failure by adding artificial index that will fail in required way.
     *
     * @see H2TreeIndexBase#find(Session, SearchRow, SearchRow)
     */
    @Test
    public void testIndexFindFail() throws Exception {
        cleanPersistenceDir();

        prepareGridForTest();

        injectTestSystemOut();

        addBadIndex();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertTrue(!lsnrs.isEmpty());

        LogListener lsnrWithError = lsnrs.stream()
            .filter(LogListener::check)
            .findAny()
            .orElse(null);

        assertNotNull("\"Failed to lookup key:\" message not found in ignite log", lsnrWithError);

        Pattern viErrMsgPattern = Pattern.compile(IDX_ISSUE_STR);

        assertTrue(viErrMsgPattern.matcher(testOut.toString()).find());
    }

    /**
     * Create and fill nodes.
     *
     * @throws Exception if failed to start node.
     */
    private void prepareGridForTest() throws Exception{
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid(CLIENT_NODE_NAME_PREFIX);

        createAndFillCache(client, CACHE_NAME, GROUP_NAME);
    }

    /**
     * Adds index that fails on {@code find()}.
     */
    private void addBadIndex() {
        IgniteEx ignite = grid(0);

        int grpId = CU.cacheGroupId(CACHE_NAME, GROUP_NAME);

        CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

        assertNotNull(grpCtx);

        GridQueryProcessor qry = ignite.context().query();

        IgniteH2Indexing indexing = (IgniteH2Indexing)qry.getIndexing();

        outer:
        for (GridCacheContext ctx : grpCtx.caches()) {
            Collection<GridQueryTypeDescriptor> types = qry.types(ctx.name());

            if (!F.isEmpty(types)) {
                for (GridQueryTypeDescriptor type : types) {
                    GridH2Table gridH2Tbl = indexing.schemaManager().dataTable(ctx.name(), type.tableName());

                    if (gridH2Tbl == null)
                        continue;

                    ArrayList<Index> indexes = gridH2Tbl.getIndexes();

                    BadIndex bi = null;

                    for (Index idx : indexes) {
                        if (idx instanceof H2TreeIndexBase) {
                            bi = new BadIndex(gridH2Tbl);

                            break;
                        }
                    }

                    if (bi != null) {
                        indexes.add(bi);

                        break outer;
                    }
                }
            }
        }
    }

    /**
     * Artificial index that throws exception on {@code find()}.
     */
    private class BadIndex extends H2TreeIndexBase {
        /**
         * Constructor.
         */
        protected BadIndex(GridH2Table tbl) {
            super(tbl);
        }

        /** */
        @Override public int inlineSize() {
            return 0;
        }

        /** */
        @Override public H2CacheRow put(H2CacheRow row) {
            return null;
        }

        /** */
        @Override public boolean putx(H2CacheRow row) {
            return false;
        }

        /** */
        @Override public boolean removex(SearchRow row) {
            return false;
        }

        /** */
        @Override public int segmentsCount() {
            return 0;
        }

        /** */
        @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
            return 0;
        }

        /** */
        @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
            throw new RuntimeException(EXCEPTION_MSG);
        }

        /** */
        @Override public Cursor findFirstOrLast(Session session, boolean first) {
            return null;
        }

        /** */
        @Override public long getRowCount(Session session) {
            return 0;
        }
    }
}

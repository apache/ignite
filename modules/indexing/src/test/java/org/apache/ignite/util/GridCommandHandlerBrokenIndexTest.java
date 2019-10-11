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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * Tests failed start of iteration through index.
 */
public class GridCommandHandlerBrokenIndexTest extends GridCommandHandlerClusterPerMethodAbstractTest{
    /** */
    private final String IDX_NAME = "bad_index";

    /** */
    private final String EXCEPTION_MSG = "Exception from BadIndex#find()";

    /** */
    private final String IDX_ISSUE_STR = "IndexValidationIssue [key=null, cacheName=" + CACHE_NAME + ", idxName=" + IDX_NAME + "], class java.lang.RuntimeException: " + EXCEPTION_MSG;

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

        String errMsg = "Find in index failed: " + IDX_ISSUE_STR;

        LogListener lsnr = LogListener.matches(errMsg).build();

        testLog.registerListener(lsnr);

        lsnrs.add(lsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /**
     * Tests Cursor initialisation failure by adding artificial index that will fail in required way.
     *
     * @see H2TreeIndex#find(Session, SearchRow, SearchRow)
     */
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

        assertNotNull("\"Find in index failed:\" message not found in ignite log", lsnrWithError);

        assertContains(log, testOut.toString(), IDX_ISSUE_STR);
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
    private void addBadIndex() throws IgniteCheckedException {
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
                    GridH2Table gridH2Tbl = indexing.dataTable(ctx.name(), type.tableName());

                    if (gridH2Tbl == null)
                        continue;

                    ArrayList<Index> indexes = gridH2Tbl.getIndexes();

                    BadIndex bi = null;

                    for (Index idx : indexes) {
                        if (idx instanceof H2TreeIndex) {
                            IndexColumn keyCol = gridH2Tbl.indexColumn(KEY_COL, SortOrder.ASCENDING);
                            IndexColumn affCol = gridH2Tbl.getAffinityKeyColumn();

                            List<IndexColumn> colsList = H2Utils.treeIndexColumns(gridH2Tbl.rowDescriptor(),
                                                            new ArrayList<IndexColumn>(2), keyCol, affCol);

                            bi = new BadIndex(ctx, null, gridH2Tbl, IDX_NAME, true, colsList, ((H2TreeIndex)idx).inlineSize(), 1, log);

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
    private class BadIndex extends H2TreeIndex {

        /**
         * @param cctx Cache context.
         * @param rowCache Row cache.
         * @param tbl Table.
         * @param name Index name.
         * @param pk Primary key.
         * @param colsList Index columns.
         * @param inlineSize Inline size.
         * @param segmentsCnt number of tree's segments.
         * @param log Logger.
         * @throws IgniteCheckedException If failed.
         */
        public BadIndex(
            GridCacheContext<?, ?> cctx,
            @Nullable H2RowCache rowCache,
            GridH2Table tbl,
            String name,
            boolean pk,
            List<IndexColumn> colsList,
            int inlineSize,
            int segmentsCnt,
            IgniteLogger log
        )
            throws IgniteCheckedException
        {
            super(cctx, rowCache, tbl, name, pk, colsList, inlineSize, segmentsCnt, log);
        }

        /** */
        @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
            throw new RuntimeException(EXCEPTION_MSG);
        }
    }
}

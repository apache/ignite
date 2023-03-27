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
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.QueryIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * Tests failed start of iteration through index.
 */
public class GridCommandHandlerBrokenIndexTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private final String IDX_NAME = "bad_index";

    /** */
    private static final String EXCEPTION_MSG = "Exception from BadIndex#find";

    /** */
    private static final String IDX_ISSUE_STR = "IndexValidationIssue \\[key=[0-9]*, cacheName=" + CACHE_NAME +
        ", idxName=bad_index], class java.lang.RuntimeException: " + EXCEPTION_MSG;

    /** */
    private List<LogListener> lsnrs = new ArrayList<>();

    /**
     * Adds error message listeners to server nodes.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode())
            return cfg;

        ListeningTestLogger testLog = new ListeningTestLogger(log);

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
     * @see InlineIndexImpl#find(IndexRow, IndexRow, boolean, boolean, int, IndexQueryContext)
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
    private void prepareGridForTest() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

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

        for (GridCacheContext<?, ?> ctx : grpCtx.caches()) {
            for (Index idx : ignite.context().indexProcessor().indexes(ctx.name())) {
                InlineIndexImpl idx0 = idx.unwrap(InlineIndexImpl.class);

                if (idx0 != null) {
                    SortedIndexDefinition idxDef = idx0.indexDefinition();
                    IndexName idxName = idxDef.idxName();

                    SortedIndexDefinition badIdxDef = new QueryIndexDefinition(
                        idxDef.typeDescriptor(),
                        idxDef.cacheInfo(),
                        new IndexName(idxName.cacheName(), idxName.schemaName(), idxName.tableName(), IDX_NAME),
                        idxDef.treeName(),
                        idxDef.idxRowCache(),
                        idxDef.primary(),
                        idxDef.affinity(),
                        idxDef.indexKeyDefinitions(),
                        idxDef.inlineSize(),
                        idxDef.keyTypeSettings()
                    );

                    ignite.context().indexProcessor().createIndex(
                        ctx,
                        (c, d) -> new BadIndex(c, (SortedIndexDefinition)d, new InlineIndexTree[] {idx0.segment(0)}),
                        badIdxDef
                    );

                    return;
                }
            }
        }
    }

    /**
     * Artificial index that throws exception on {@code find()}.
     */
    private static class BadIndex extends InlineIndexImpl {
        /** */
        public BadIndex(GridCacheContext<?, ?> cctx, SortedIndexDefinition def, InlineIndexTree[] segments) {
            super(cctx, def, segments, null);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<IndexRow> find(
            IndexRow lower,
            IndexRow upper,
            boolean lowIncl,
            boolean upIncl,
            int segment,
            IndexQueryContext qryCtx
        ) {
            throw new RuntimeException(EXCEPTION_MSG);
        }
    }
}

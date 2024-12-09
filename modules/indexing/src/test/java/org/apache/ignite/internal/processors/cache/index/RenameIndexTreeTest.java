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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.IndexRenameRootPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.findIndexRootPages;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask.renameIndexRootPages;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INDEX_ROOT_PAGE_RENAME_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl.MAX_IDX_NAME_LEN;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.cacheContext;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing index tree renaming.
 */
public class RenameIndexTreeTest extends AbstractRebuildIndexTest {
    /**
     * Checking the correct renaming of the index root pages.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        int segments = idxDef.segments();

        String oldTreeName = idxDef.treeName();
        assertExistIndexRoot(cache, oldTreeName, segments, true);

        String newTreeName = UUID.randomUUID().toString();

        // There will be no renaming.
        assertExistIndexRoot(cache, newTreeName, segments, false);
        assertTrue(renameIndexRoot(cache, newTreeName, newTreeName, segments).isEmpty());

        // Checking the validation of the new index name.
        String moreMaxLenName = Arrays.toString(new byte[MAX_IDX_NAME_LEN + 1]);
        assertThrows(log, () -> renameIndexRoot(cache, oldTreeName, moreMaxLenName, segments), Exception.class, null);

        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        assertExistIndexRoot(cache, oldTreeName, segments, false);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Checking that the renamed index root pages after the checkpoint will be
     * correctly restored and found after the node is restarted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        String oldTreeName = idxDef.treeName();
        String newTreeName = UUID.randomUUID().toString();

        int segments = idxDef.segments();
        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        forceCheckpoint();

        stopGrid(0);

        n = startGrid(0);
        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, true);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Checking that if we rename the index root pages without a checkpoint,
     * then after restarting the node we will not find them.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotPersistRenamingIndexRootPage() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        enableCheckpoints(n, getTestIgniteInstanceName(), false);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        String oldTreeName = idxDef.treeName();
        String newTreeName = UUID.randomUUID().toString();

        int segments = idxDef.segments();
        assertEquals(segments, renameIndexRoot(cache, oldTreeName, newTreeName, segments).size());

        stopGrid(0);

        n = startGrid(0);
        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, true);
        assertExistIndexRoot(cache, newTreeName, segments, false);
    }

    /**
     * Checking applying {@link IndexRenameRootPageRecord}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexRenameRootPageRecord() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        enableCheckpoints(n, getTestIgniteInstanceName(), false);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        String oldTreeName = idxDef.treeName();
        String newTreeName = UUID.randomUUID().toString();

        int segments = idxDef.segments();
        int cacheId = cacheContext(cache).cacheId();

        IndexRenameRootPageRecord r = new IndexRenameRootPageRecord(cacheId, oldTreeName, newTreeName, segments);
        walMgr(n).log(r);

        Set<Integer> cacheIds = n.context().cache().cacheNames().stream().map(CU::cacheId).collect(toSet());
        int fakeCacheId = cacheIds.stream().mapToInt(Integer::intValue).sum();

        while (cacheIds.contains(fakeCacheId))
            fakeCacheId++;

        // Check that the node does not crash when trying to apply a logical record with a non-existing cacheId.
        walMgr(n).log(new IndexRenameRootPageRecord(fakeCacheId, oldTreeName, newTreeName, segments));

        stopGrid(0);

        n = startGrid(0);

        cache = n.cache(DEFAULT_CACHE_NAME);

        assertExistIndexRoot(cache, oldTreeName, segments, false);
        assertExistIndexRoot(cache, newTreeName, segments, true);
    }

    /**
     * Checking the correctness of {@link DurableBackgroundCleanupIndexTreeTask#findIndexRootPages}
     * and {@link DurableBackgroundCleanupIndexTreeTask#renameIndexRootPages}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameFromTask() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        GridCacheContext<Integer, Person> cctx = cacheContext(cache);

        String oldTreeName = idxDef.treeName();
        int segments = idxDef.segments();

        assertExistIndexRoot(cache, oldTreeName, segments, true);

        Map<Integer, RootPage> rootPages0 = findIndexRoots(cache, oldTreeName, segments);
        Map<Integer, RootPage> rootPages1 = findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments);

        assertEqualsCollections(toPageIds(rootPages0), toPageIds(rootPages1));

        long currSegIdx = walMgr(n).currentSegment();

        String newTreeName = UUID.randomUUID().toString();
        renameIndexRootPages(cctx.group(), cctx.name(), oldTreeName, newTreeName, segments);

        assertExistIndexRoot(cache, oldTreeName, segments, false);
        assertExistIndexRoot(cache, newTreeName, segments, true);

        assertTrue(findIndexRootPages(cctx.group(), cctx.name(), oldTreeName, segments).isEmpty());

        rootPages0 = findIndexRoots(cache, newTreeName, segments);
        rootPages1 = findIndexRootPages(cctx.group(), cctx.name(), newTreeName, segments);

        assertEqualsCollections(toPageIds(rootPages0), toPageIds(rootPages1));

        WALPointer start = new WALPointer(currSegIdx, 0, 0);
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> pred = (t, p) -> t == INDEX_ROOT_PAGE_RENAME_RECORD;

        try (WALIterator it = walMgr(n).replay(start, pred)) {
            List<WALRecord> records = stream(it.spliterator(), false).map(IgniteBiTuple::get2).collect(toList());

            assertEquals(1, records.size());

            IndexRenameRootPageRecord record = (IndexRenameRootPageRecord)records.get(0);

            assertEquals(cctx.cacheId(), record.cacheId());
            assertEquals(oldTreeName, record.oldTreeName());
            assertEquals(newTreeName, record.newTreeName());
            assertEquals(segments, record.segments());
        }
    }

    /**
     * Tests that {@link DurableBackgroundCleanupIndexTreeTask#renameIndexTrees(CacheGroupContext)}
     * can be run before submitting the task.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRenameBeforeRunningTask() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        InlineIndexImpl idx = (InlineIndexImpl)index(n, cache, idxName);

        GridCacheContext<Integer, Person> cctx = cacheContext(cache);

        InlineIndexTree[] segments = getFieldValue(idx, "segments");

        DurableBackgroundCleanupIndexTreeTask task = new DurableBackgroundCleanupIndexTreeTask(
            cctx.group().name(),
            cctx.name(),
            idxName,
            idx.indexDefinition().treeName(),
            UUID.randomUUID().toString(),
            segments.length,
            segments
        );

        assertTrue(task.needToRename());

        task.renameIndexTrees(cctx.group());

        assertFalse(task.needToRename());
    }

    /**
     * Renaming index trees.
     *
     * @param cache Cache.
     * @param oldTreeName Old index tree name.
     * @param newTreeName Old index tree name.
     * @param segments Segment count.
     * @return Root pages of renamed trees.
     * @throws Exception If failed.
     */
    private Collection<RootPage> renameIndexRoot(
        IgniteCache<Integer, Person> cache,
        String oldTreeName,
        String newTreeName,
        int segments
    ) throws Exception {
        GridCacheContext<Integer, Person> cacheCtx = cacheContext(cache);

        List<RootPage> res = new ArrayList<>();

        cacheCtx.shared().database().checkpointReadLock();

        try {
            for (int i = 0; i < segments; i++) {
                RootPage rootPage = cacheCtx.offheap().renameRootPageForIndex(
                    cacheCtx.cacheId(),
                    oldTreeName,
                    newTreeName,
                    i
                );

                if (rootPage != null)
                    res.add(rootPage);
            }
        }
        finally {
            cacheCtx.shared().database().checkpointReadUnlock();
        }

        return res;
    }

    /**
     * Checking for the existence of root pages for an index.
     *
     * @param cache Cache.
     * @param treeName Index tree name.
     * @param segments Segment count.
     * @param expExist Expectation of existence.
     * @throws Exception If failed.
     */
    private void assertExistIndexRoot(
        IgniteCache<Integer, Person> cache,
        String treeName,
        int segments,
        boolean expExist
    ) throws Exception {
        Map<Integer, RootPage> rootPages = findIndexRoots(cache, treeName, segments);

        for (int i = 0; i < segments; i++)
            assertEquals(expExist, rootPages.containsKey(i));
    }

    /**
     * Finding index root pages.
     *
     * @param cache Cache.
     * @param treeName Index tree name.
     * @param segments Segment count.
     * @return Mapping: segment number -> index root page.
     * @throws Exception If failed.
     */
    private Map<Integer, RootPage> findIndexRoots(
        IgniteCache<Integer, Person> cache,
        String treeName,
        int segments
    ) throws Exception {
        GridCacheContext<Integer, Person> cacheCtx = cacheContext(cache);

        Map<Integer, RootPage> rootPages = new HashMap<>();

        for (int i = 0; i < segments; i++) {
            RootPage rootPage = cacheCtx.offheap().findRootPageForIndex(cacheCtx.cacheId(), treeName, i);

            if (rootPage != null)
                rootPages.put(i, rootPage);
        }

        return rootPages;
    }

    /**
     * Getting page ids.
     *
     * @param rootPages Mapping: segment number -> index root page.
     * @return Page ids sorted by segment numbers.
     */
    private Collection<FullPageId> toPageIds(Map<Integer, RootPage> rootPages) {
        return rootPages.entrySet().stream()
            .sorted(Map.Entry.comparingByKey()).map(e -> e.getValue().pageId()).collect(toList());
    }
}

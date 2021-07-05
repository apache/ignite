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
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

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
     * Checking that if the renaming of root pages is not a checkpoint,
     * then after restarting the node they will not be found.
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
        GridCacheContext<Integer, Person> cacheCtx = cacheContext(cache);

        for (int i = 0; i < segments; i++) {
            RootPage rootPage = cacheCtx.offheap().findRootPageForIndex(cacheCtx.cacheId(), treeName, i);

            assertEquals(expExist, rootPage != null);
        }
    }

    /**
     * Getting index description.
     *
     * @param idx Index.
     * @return Index description.
     */
    private SortedIndexDefinition indexDefinition(Index idx) {
        return getFieldValue(idx, "def");
    }

    /**
     * Getting the cache index.
     *
     * @param n Node.
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index.
     */
    @Nullable private Index index(IgniteEx n, IgniteCache<Integer, Person> cache, String idxName) {
        return n.context().indexProcessor().indexes(cacheContext(cache)).stream()
            .filter(i -> idxName.equals(i.name()))
            .findAny()
            .orElse(null);
    }
}

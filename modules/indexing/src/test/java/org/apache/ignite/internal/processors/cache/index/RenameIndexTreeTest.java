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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.cacheContext;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing index tree renaming.
 */
public class RenameIndexTreeTest extends AbstractRebuildIndexTest {
    @Test
    public void test0() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);
        populate(cache, 1_000);

        String idxName = "IDX0";
        createIdx(cache, idxName);

        SortedIndexDefinition idxDef = indexDefinition(index(n, cache, idxName));

        assertExistIndexRoot(cache, idxDef.treeName(), idxDef.segments(), true);

        // TODO: 02.07.2021 continue
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

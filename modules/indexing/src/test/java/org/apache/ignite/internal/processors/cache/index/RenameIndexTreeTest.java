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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

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

        // TODO: 02.07.2021 continue

        log.warning("idxDef=" + idxDef);
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
        return n.context().indexProcessor().indexes(n.cachex(cache.getName()).context()).stream()
            .filter(i -> idxName.equals(i.name()))
            .findAny()
            .orElse(null);
    }
}

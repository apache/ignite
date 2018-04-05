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

package org.apache.ignite.internal.processors.cache.datastructures.replicated;

import java.util.UUID;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheMultimapApiSelfAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

public class GridCacheReplicatedAtomicCollocatedMultimapApiSelfTest extends GridCacheMultimapApiSelfAbstractTest {

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected boolean collocated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void testMultimapCollocationMode() throws Exception {
        String multimapName = UUID.randomUUID().toString();
        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(getMultimapBackingCache(multimap).name().startsWith("datastructures_" + collectionCacheAtomicityMode() + "_" + collectionCacheMode()));
        assertTrue(multimap.collocated());
    }
}

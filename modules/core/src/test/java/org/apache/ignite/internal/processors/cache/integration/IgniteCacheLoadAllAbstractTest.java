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

package org.apache.ignite.internal.processors.cache.integration;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.grid.cache.affinity.*;

import javax.cache.integration.*;
import java.util.*;

/**
 * Test for {@link javax.cache.Cache#loadAll(Set, boolean, CompletionListener)}.
 */
public abstract class IgniteCacheLoadAllAbstractTest extends IgniteCacheAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testLoadAll() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        for (int i = 0; i < 1000; i++)
            cache.put(i, String.valueOf(i));

        stopAllGrids();

        startGrids();

        cache = jcache(0);

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 100; i++)
            keys.add(i);

        Set<Integer> nonExistKeys = new HashSet<>();

        for (int i = 10_000; i < 10_010; i++)
            nonExistKeys.add(i);

        keys.addAll(nonExistKeys);

        CompletionListener lsnr = new CompletionListenerFuture();

        cache.loadAll(keys, false, lsnr);

        GridCacheAffinity<Object> aff = cache(0).affinity();

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = ignite(i).cluster().localNode();

            IgniteCache<Integer, String> cache0 = jcache(i);

            for (int key = 0; key < 1000; key++) {
                String expVal = (keys.contains(key) && !nonExistKeys.contains(key)) ? String.valueOf(key) : null;

                if (aff.isPrimaryOrBackup(node, key)) {
                    assertEquals(expVal, cache0.localPeek(key));

                    assertEquals(expVal, cache0.get(key));
                } else {
                    assertNull(cache0.localPeek(key));

                    assertNull(cache0.get(key));
                }

            }
        }
    }
}

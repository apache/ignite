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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** */
public class PageMemoryLazyAllocationWithPDSTest extends PageMemoryLazyAllocationTest {
    /** {@inheritDoc} */
    @Override public void testLocalCacheOnClientNodeWithLazyAllocation() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-11677");
    }

    /** */
    @Test
    public void testNodeRestart() throws Exception {
        lazyAllocation = true;

        client = false;

        IgniteEx srv = startSrv();

        createCacheAndPut(srv);

        stopAllGrids(false);

        IgniteCache<Integer, String> cache = startSrv().cache("my-cache-2");

        assertEquals(cache.get(1), "test");
    }

    /** */
    @Test
    public void testClientNodeRestart() throws Exception {
        lazyAllocation = true;

        client = false;

        IgniteEx srv = startSrv();

        client = true;

        IgniteEx clnt = startGrid(1);

        createCacheAndPut(clnt);

        stopAllGrids(false);

        client = false;

        srv = startSrv();

        client = true;

        IgniteCache<Integer, String> cache = startGrid(1).cache("my-cache-2");

        assertEquals(cache.get(1), "test");
    }

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }
}

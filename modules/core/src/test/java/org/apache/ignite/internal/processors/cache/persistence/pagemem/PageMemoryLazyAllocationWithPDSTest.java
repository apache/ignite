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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.GB;

/** */
public class PageMemoryLazyAllocationWithPDSTest extends PageMemoryLazyAllocationTest {

    public static final long PETA_BYTE = 1024 * GB;

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11677")
    @Override public void testLocalCacheOnClientNodeWithLazyAllocation() throws Exception {
        // No-op.
    }

    /** */
    @Test
    public void testNodeRestart() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startSrv()[0];

        createCacheAndPut(srv);

        stopAllGrids(false);

        IgniteCache<Integer, String> cache = startSrv()[0].cache("my-cache-2");

        assertEquals("test", cache.get(1));
    }

    /** */
    @Test
    public void testClientNodeRestart() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startSrv()[0];

        IgniteEx clnt = startClientGrid(2);

        createCacheAndPut(clnt);

        stopAllGrids(false);

        srv = startSrv()[0];

        IgniteCache<Integer, String> cache = startClientGrid(2).cache("my-cache-2");

        assertEquals("test", cache.get(1));
    }

    /** */
    @Test
    public void testHugeNotUsedMemoryRegion() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startGrid(cfgWithHugeRegion("test-server"));

        startGrid(cfgWithHugeRegion("test-server-2"));

        srv.cluster().active(true);

        awaitPartitionMapExchange();

        stopAllGrids(false);

        srv = startGrid(cfgWithHugeRegion("test-server"));

        startGrid(cfgWithHugeRegion("test-server-2"));

        srv.cluster().active(true);
    }

    /** */
    @Test
    public void testCreateCacheFailsInHugeMemoryRegion() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startGrid(cfgWithHugeRegion("test-server")
            .setFailureHandler(new StopNodeFailureHandler()));

        srv.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** */
    @Test
    public void testCreateCacheFromClientFailsInHugeMemoryRegion() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startGrid(cfgWithHugeRegion("test-server")
            .setFailureHandler(new StopNodeFailureHandler()));

        IgniteEx clnt = startClientGrid(cfgWithHugeRegion("test-client")
            .setFailureHandler(new StopNodeFailureHandler()));

        srv.cluster().active(true);

        awaitPartitionMapExchange();
    }

    @NotNull private IgniteConfiguration cfgWithHugeRegion(String name) throws Exception {
        IgniteConfiguration cfg = getConfiguration(name);

        for (DataRegionConfiguration drc : cfg.getDataStorageConfiguration().getDataRegionConfigurations()) {
            if (drc.getName().equals(LAZY_REGION))
                drc.setMaxSize(PETA_BYTE);
        }
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }
}

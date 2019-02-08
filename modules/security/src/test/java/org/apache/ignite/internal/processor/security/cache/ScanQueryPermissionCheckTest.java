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

package org.apache.ignite.internal.processor.security.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;

/**
 * Test cache permission for invoking of Scan Query.
 */
@RunWith(JUnit4.class)
public class ScanQueryPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /**
     * @throws Exception If fail.
     */
    @Test
    public void testServerNode() throws Exception {
        testScanQuery(false);
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testClientNode() throws Exception {
        testScanQuery(true);
    }

    /**
     * @param isClient True if is client mode.
     * @throws Exception If failed.
     */
    private void testScanQuery(boolean isClient) throws Exception {
        putTestData();

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), isClient);

        assertFalse(node.cache(CACHE_NAME).query(new ScanQuery<String, Integer>()).getAll().isEmpty());
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).query(new ScanQuery<String, Integer>()).getAll());
    }

    /**
     *
     */
    private void putTestData() {
        Ignite ignite = G.allGrids().stream().findFirst().get();

        try (IgniteDataStreamer<String, Integer> strAllowedCache = ignite.dataStreamer(CACHE_NAME);
             IgniteDataStreamer<String, Integer> strForbiddenCache = ignite.dataStreamer(FORBIDDEN_CACHE)) {
            for (int i = 1; i <= 10; i++) {
                strAllowedCache.addData(Integer.toString(i), i);
                strForbiddenCache.addData(Integer.toString(i), i);
            }
        }
    }
}

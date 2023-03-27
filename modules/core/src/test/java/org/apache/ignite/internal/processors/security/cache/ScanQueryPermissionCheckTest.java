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

package org.apache.ignite.internal.processors.security.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test cache permission for invoking of Scan Query.
 */
@RunWith(Parameterized.class)
public class ScanQueryPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** Parameters. */
    @Parameters(name = "clientMode={0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(new Boolean[] {true}, new Boolean[] {false});
    }

    /** Client mode. */
    @Parameter()
    public boolean clientMode;

    /** */
    @Test
    public void testScanQuery() throws Exception {
        Ignite ignite = G.allGrids().stream().findFirst().orElseThrow(IllegalStateException::new);

        try (IgniteDataStreamer<String, Integer> strAllowedCache = ignite.dataStreamer(CACHE_NAME);
             IgniteDataStreamer<String, Integer> strForbiddenCache = ignite.dataStreamer(FORBIDDEN_CACHE)) {
            for (int i = 1; i <= 10; i++) {
                strAllowedCache.addData(Integer.toString(i), i);
                strForbiddenCache.addData(Integer.toString(i), i);
            }
        }

        Ignite node = startGrid(loginPrefix(clientMode) + "_test_node",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), clientMode);

        assertFalse(node.cache(CACHE_NAME).query(new ScanQuery<String, Integer>()).getAll().isEmpty());

        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).query(new ScanQuery<String, Integer>()).getAll(),
            SecurityException.class);
    }
}

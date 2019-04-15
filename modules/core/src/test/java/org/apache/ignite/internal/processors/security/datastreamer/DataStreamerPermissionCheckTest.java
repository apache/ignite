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

package org.apache.ignite.internal.processors.security.datastreamer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Test cache permissions for Data Streamer.
 */
@RunWith(JUnit4.class)
public class DataStreamerPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /**
     * @throws Exception If fail.
     */
    @Test
    public void testServerNode() throws Exception {
        testDataStreamer(false);
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testClientNode() throws Exception {
        testDataStreamer(true);
    }

    /**
     * @param isClient True if is client mode.
     * @throws Exception If fail.
     */
    private void testDataStreamer(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
            builder()
                .appendCachePermissions(CACHE_NAME, SecurityPermission.CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, SecurityPermission.CACHE_READ)
                .build(), isClient);

        List<Consumer<IgniteDataStreamer<String, Integer>>> operations = Arrays.asList(
            s -> s.addData("k", 1),
            s -> s.addData(singletonMap("key", 2)),
            s -> s.addData((Map.Entry<String, Integer>)entry()),
            s -> s.addData(singletonList(entry())));

        operations.forEach(c -> assertAllowed(node, c));

        operations.forEach(c -> assertForbidden(node, c));
    }

    /**
     * @param node Node.
     * @param c Consumer.
     */
    private void assertAllowed(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> c) {
        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(CACHE_NAME)) {
            c.accept(s);
        }
    }

    /**
     * @param node Node.
     * @param c Consumer.
     */
    private void assertForbidden(Ignite node, Consumer<IgniteDataStreamer<String, Integer>> c) {
        assertForbidden(() -> {
                try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(FORBIDDEN_CACHE)) {
                    c.accept(s);
                }
            }
        );
    }
}

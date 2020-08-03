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
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test cache permissions for Data Streamer.
 */
@RunWith(Parameterized.class)
public class DataStreamerPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
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
    public void testDataStreamer() throws Exception {
        Ignite node = startGrid(loginPrefix(clientMode) + "_test_node",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, SecurityPermission.CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, SecurityPermission.CACHE_READ)
                .build(), clientMode);

        List<Consumer<IgniteDataStreamer<String, Integer>>> ops = Arrays.asList(
            s -> s.addData("k", 1),
            s -> s.addData(singletonMap("key", 2)),
            s -> s.addData((Map.Entry<String, Integer>)entry()),
            s -> s.addData(singletonList(entry())));

        ops.forEach(c -> runOperation(node, CACHE_NAME, c));

        ops.forEach(c ->
            assertThrowsWithCause(() -> runOperation(node, FORBIDDEN_CACHE, c), SecurityException.class));
    }

    /**
     * @param node Node.
     * @param c Consumer.
     */
    private void runOperation(Ignite node, String cache, Consumer<IgniteDataStreamer<String, Integer>> c) {
        try (IgniteDataStreamer<String, Integer> s = node.dataStreamer(cache)) {
            c.accept(s);
        }
    }
}

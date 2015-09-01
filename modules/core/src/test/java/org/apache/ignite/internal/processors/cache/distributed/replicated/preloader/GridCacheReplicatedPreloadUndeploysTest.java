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

package org.apache.ignite.internal.processors.cache.distributed.replicated.preloader;

import java.util.HashMap;
import java.util.Map;
import javax.swing.JOptionPane;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Test for replicated cache preloader and concurrent undeploys.
 */
public class GridCacheReplicatedPreloadUndeploysTest {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = G.start("examples/config/example-cache.xml")) {
            if (g.cluster().forRemotes().nodes().isEmpty()) {
                X.print(">>> This test needs 1 remote node at start " +
                    "and addition of 1 more node at the end.");

                return;
            }

            X.println(">>> Beginning data population...");

            int cnt = 10000;

            Map<Integer, SampleValue> map = null;

            for (int i = 0; i < cnt; i++) {
                if (i % 200 == 0) {
                    if (map != null && !map.isEmpty()) {
                        g.cache("replicated").putAll(map);

                        X.println(">>> Put entries count: " + i);
                    }

                    map = new HashMap<>();
                }

                map.put(i, new SampleValue());
            }

            if (map != null && !map.isEmpty()) {
                g.cache("replicated").putAll(map);

                X.println(">>> Put entries count: " + cnt);
            }

            JOptionPane.showMessageDialog(null, "Start one more node now and press OK " +
                "while new node is preloading.");
        }
    }

    /**
     *
     */
    private GridCacheReplicatedPreloadUndeploysTest() {
        // No-op.
    }

    /**
     *
     */
    private static class SampleValue {
        // No-op.
    }
}
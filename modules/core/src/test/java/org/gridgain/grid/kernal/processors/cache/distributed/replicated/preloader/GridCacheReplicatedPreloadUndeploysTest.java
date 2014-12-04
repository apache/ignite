/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

import javax.swing.*;
import java.util.*;

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

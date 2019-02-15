/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.IgniteState.STOPPED;

/**
 * Tests for {@link org.apache.ignite.Ignition}.
 */
public class GridFactoryVmShutdownTest {
    /**
     *
     */
    private GridFactoryVmShutdownTest() {
        // No-op.
    }

    /**
     * @param args Args (optional).
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final ConcurrentMap<String, IgniteState> states = new ConcurrentHashMap<>();

        G.addListener(new IgnitionListener() {
            @Override public void onStateChange(@Nullable String name, IgniteState state) {
                if (state == STARTED) {
                    IgniteState state0 = states.put(maskNull(name), STARTED);

                    assert state0 == null;
                }
                else {
                    assert state == STOPPED;

                    boolean replaced = states.replace(maskNull(name), STARTED, STOPPED);

                    assert replaced;
                }
            }
        });

        // Test with shutdown hook enabled and disabled.
        // System.setProperty(GridSystemProperties.IGNITE_NO_SHUTDOWN_HOOK, "true");

        // Grid will start and add shutdown hook.
        G.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override public void run() {
                IgniteConfiguration cfg = new IgniteConfiguration();

                cfg.setIgniteInstanceName("test1");

                try {
                    G.start(cfg);
                }
                finally {
                    X.println("States: " + states);
                }
            }
        }));

        System.exit(0);
    }

    /**
     * Masks {@code null} string.
     *
     * @param s String to mask.
     * @return Mask value or string itself if it is not {@code null}.
     */
    private static String maskNull(String s) {
        return s != null ? s : "null-mask-8AE34BF8";
    }
}
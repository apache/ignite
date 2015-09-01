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

                cfg.setGridName("test1");

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
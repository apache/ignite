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

package org.apache.ignite.startup.cmdline;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.IgniteState.STARTED;

/**
 * Command line loader test.
 */
@GridCommonTest(group = "Loaders")
public class GridCommandLineLoaderTest extends GridCommonAbstractTest {
    /** */
    private static final String GRID_CFG_PATH = "/modules/core/src/test/config/loaders/grid-cfg.xml";

    /** */
    private final CountDownLatch latch = new CountDownLatch(2);

    /** */
    public GridCommandLineLoaderTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoader() throws Exception {
        String path = U.getIgniteHome() + GRID_CFG_PATH;

        info("Loading Grid from configuration file: " + path);

        G.addListener(new IgnitionListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                if (state == STARTED) {
                    info("Received started notification from grid: " + name);

                    latch.countDown();

                    G.stop(name, true);
                }
            }
        });

        CommandLineStartup.main(new String[]{path});
    }
}
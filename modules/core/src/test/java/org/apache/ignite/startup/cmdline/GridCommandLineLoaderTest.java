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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_RESTART_CODE;

/**
 * Command line loader test.
 */
@GridCommonTest(group = "Loaders")
public class GridCommandLineLoaderTest extends GridCommonAbstractTest {
    /** */
    private static final String GRID_CFG_PATH = "/modules/core/src/test/config/loaders/grid-cfg.xml";

    /**
     * @throws Exception If failed.
     */
    public void testLoader() throws Exception {
        String path = U.getIgniteHome() + GRID_CFG_PATH;

        info("Using Grids from configuration file: " + path);

        IgniteProcessProxy proxy = new IgniteProcessProxy(
            new IgniteConfiguration().setIgniteInstanceName("fake"), log, null) {
                @Override protected String igniteNodeRunnerClassName() throws Exception {
                    return CommandLineStartup.class.getCanonicalName();
                }

                @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                    return path;
                }
            };

        try {
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !proxy.getProcess().getProcess().isAlive();
                }
            }, 150_000);
        }
        finally {
            if (proxy.getProcess().getProcess().isAlive())
                proxy.kill();
        }

        assertEquals(2, proxy.getProcess().getProcess().exitValue());
    }

    /**
     * Kills node after it is started.
     */
    public static class KillerLifecycleBean implements LifecycleBean {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
            if (evt == LifecycleEventType.AFTER_NODE_START) {
                System.setProperty(IGNITE_RESTART_CODE, Integer.toString(
                    1 + IgniteSystemProperties.getInteger(IGNITE_RESTART_CODE, 0)));

                System.out.println("Ignite instance seen, will shut it down.");

                new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            Thread.sleep(3000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("Shutdown imminent.");

                        ignite.close();
                    }
                }).start();
            }
        }
    }
}

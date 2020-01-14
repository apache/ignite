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

package org.apache.ignite.p2p;

import java.lang.reflect.Constructor;
import java.net.URL;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class DeploymentClassLoaderCallableTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalCallable";

    /** */
    private static final String RUN_CLS1 = "org.apache.ignite.tests.p2p.compute.ExternalCallable1";

    /** */
    private static final String RUN_CLS2 = "org.apache.ignite.tests.p2p.compute.ExternalCallable2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDeploymentFromSecondAndThird() throws Exception {
        try {
            startGrid(1);

            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            runJob0(ignite2, 10_000);

            runJob1(ignite3, 10_000);
            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDeploymentFromEach() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            runJob0(ignite1, 10_000);

            runJob1(ignite2, 10_000);

            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDeploymentFromOne() throws Exception {
        try {
            startGrid(1);
            startGrid(2);

            final Ignite ignite3 = startGrid(3);

            runJob0(ignite3, 10_000);
            runJob1(ignite3, 10_000);
            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void runJob1(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader1 = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS2);

        Constructor ctor = testClassLoader1.loadClass(RUN_CLS1).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<?>)ctor.newInstance());
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void runJob0(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS1, RUN_CLS2);

        Constructor ctor = testClassLoader.loadClass(RUN_CLS).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<?>)ctor.newInstance());
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void runJob2(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS1);

        Constructor ctor = testClassLoader.loadClass(RUN_CLS2).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<?>)ctor.newInstance());
    }
}

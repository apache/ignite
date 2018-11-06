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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Collection;

/**
 */
public class SharedDeploymentTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalCallable";

    /** */
    private static final String RUN_CLS1 = "org.apache.ignite.tests.p2p.compute.ExternalCallable1";

    /** */
    private static final String RUN_CLS2 = "org.apache.ignite.tests.p2p.compute.ExternalCallable2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception if failed.
     */
    public void testDeploymentFromSecondAndThird() throws Exception {
        try {
            startGrid(1);

            final Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Collection<Object> res = runJob0(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS1/*, RUN_CLS2*/), ignite2, 10_000, 1);

            for (Object o: res)
                assertEquals(o, 42);

            res = runJob1(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS2), ignite3, 10_000, 2);

            for (Object o: res)
                assertEquals(o, 42);

            res = runJob2(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS1), ignite3, 10_000, 3);

            for (Object o: res)
                assertEquals(o, 42);

            ignite3.close();

            ignite3 = startGrid(3);

            res = runJob2(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls.second"))}, RUN_CLS, RUN_CLS1), ignite3, 10_000, 4);

            for (Object o: res)
                assertEquals(o, 43);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @param param Parameter.
     * @throws Exception If failed.
     */
    private Collection<Object> runJob1(ClassLoader testClassLoader, Ignite ignite, long timeout, int param) throws Exception {
        Constructor ctor = testClassLoader.loadClass(RUN_CLS1).getConstructor(int.class);

        return ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<Object>)ctor.newInstance(param));
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @param param Parameter.
     * @throws Exception If failed.
     */
    private Collection<Object> runJob0(ClassLoader testClassLoader, Ignite ignite, long timeout, int param) throws Exception {
        Constructor ctor = testClassLoader.loadClass(RUN_CLS).getConstructor(int.class);

        return ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<Object>)ctor.newInstance(param));
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @param param Parameter.
     * @throws Exception If failed.
     */
    private Collection<Object> runJob2(ClassLoader testClassLoader, Ignite ignite, long timeout, int param) throws Exception {
        Constructor ctor = testClassLoader.loadClass(RUN_CLS2).getConstructor(int.class);

        return ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<Object>)ctor.newInstance(param));
    }
}

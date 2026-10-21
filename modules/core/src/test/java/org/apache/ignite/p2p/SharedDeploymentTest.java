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
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class SharedDeploymentTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalCallable";

    /** */
    private static final String RUN_CLS1 = "org.apache.ignite.tests.p2p.compute.ExternalCallable1";

    /** */
    private static final String RUN_CLS2 = "org.apache.ignite.tests.p2p.compute.ExternalCallable2";

    /** */
    public static final String RUN_LAMBDA = "org.apache.ignite.tests.p2p.compute.ExternalLambda";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(DeploymentMode.SHARED);
    }

    /**
     * Start a cluster of 3 server nodes.
     * Run callables deployed on nodes 2 and 3 from the first classpath. Check return values.
     * Stop nodes 2 and 3 (all master nodes of the deployment), restart node 3 and run a callable deployed on it
     * from the second classpath (another version of the same class). Check the return value of the new version.
     * <br><br>
     * According to SHARED mode contract: <br>
     * 1. Classes from different master nodes with the same user version share
     * the same class loader on worker nodes <br>
     *
     * 2. Classes are un-deployed when all master nodes leave the cluster or the
     * user version changes <br><br>
     *
     * The documented contract is: "classes from different master nodes with the
     * same user version share the same class
     * loader on worker nodes. Classes are un-deployed when all master nodes
     * leave the cluster or the user version changes."
     * Also from the user-version section: redeploying changed code on a subset
     * of nodes requires changing the user version.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDeploymentFromSecondAndThird() throws Exception {
        try {
            startGrid(1);

            final Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Collection<Object> res = runJob0(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS1/*, RUN_CLS2*/), ignite2, 10_000, 1);

            // Node 3 and node 2 are different nodes with the same user
            // version, they share the loader, node 3 gets v1 = 42.
            for (Object o: res)
                assertEquals(o, 42);

            res = runJob1(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS2), ignite3, 10_000, 2);

            // Nothing is undeployed: user version unchanged.
            for (Object o: res)
                assertEquals(o, 42);

            res = runJob2(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS1), ignite3, 10_000, 3);

            for (Object o: res)
                assertEquals(o, 42);

            // In SHARED mode classes are undeployed only when all master nodes leave: node 2 participates in the
            // deployment serving the first version of the classes, so it has to leave before node 3 is restarted
            // with the second version. Otherwise node 3 would legitimately share the already deployed classes.
            stopGrid(2);

            ignite3.close();

            ignite3 = startGrid(3);

            res = runJob2(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls.second"))}, RUN_CLS, RUN_CLS1), ignite3, 10_000, 4);

            // User-version bump: redeploying changed code on a subset of nodes.
            for (Object o: res)
                assertEquals(o, 43);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Start a cluster of 3 server nodes.
     * Broadcast a lambda callable deployed on node 3 from first classpath. Check correct return values.
     * Restart node 3
     * Broadcast a lambda callable deployed on node 3 from second classpath, declared in binary incompatible class
     *  (type of field changed). Check corresponding return values.
     *
     * Expected behavior: no errors.
     * Problematic behavior: marshalling exception due to lambda capturing class binary incompatibility.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testLambdaDeploymentFromSecondAndThird() throws Exception {
        try {
            startGrid(1);
            startGrid(2);

            Ignite ignite3 = startGrid(3);

            Collection<Object> res = runLambda(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls"))}), ignite3, 10_000);

            for (Object o: res)
                assertEquals(o, 42);

            ignite3.close();

            ignite3 = startGrid(3);

            res = runLambda(new GridTestExternalClassLoader(new URL[] {
                new URL(GridTestProperties.getProperty("p2p.uri.cls.second"))}), ignite3, 10_000);

            for (Object o: res)
                assertEquals(43, o);
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

    /**
     * @throws Exception If failed.
     */
    private Collection<Object> runLambda(ClassLoader testClassLoader, Ignite ignite, long timeout) throws Exception {
        Method m = testClassLoader.loadClass(RUN_LAMBDA).getMethod("lambda", new Class[0]);

        return ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<Object>)m.invoke(null, new Object[0]));
    }
}

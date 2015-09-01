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

package org.apache.ignite.gridify;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;

/**
 * Abstract AOP test.
 */
@SuppressWarnings( {"OverlyStrongTypeCast", "JUnitAbstractTestClassNamingConvention", "ProhibitedExceptionDeclared", "IfMayBeConditional"})
public abstract class AbstractAopTest extends GridCommonAbstractTest {
    /** */
    private DeploymentMode depMode = DeploymentMode.PRIVATE;

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultPrivate() throws Exception {
        checkDefault(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultIsolated() throws Exception {
        checkDefault(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultContinuous() throws Exception {
        checkDefault(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultShared() throws Exception {
        checkDefault(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderPrivate() throws Exception {
        checkDefaultWithUserClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderIsolated() throws Exception {
        checkDefaultWithUserClassLoader(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderContinuous() throws Exception {
        checkDefaultWithUserClassLoader(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderShared() throws Exception {
        checkDefaultWithUserClassLoader(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderPrivate() throws Exception {
        checkSingleDeploymentWithUserClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderIsolated() throws Exception {
        checkSingleDeploymentWithUserClassLoader(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderContinuous() throws Exception {
        checkSingleDeploymentWithUserClassLoader(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderShared() throws Exception {
        checkSingleDeploymentWithUserClassLoader(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderPrivate() throws Exception {
        checkDefaultResourceWithUserClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderIsolated() throws Exception {
        checkDefaultResourceWithUserClassLoader(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderContinuous() throws Exception {
        checkDefaultResourceWithUserClassLoader(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderShared() throws Exception {
        checkDefaultResourceWithUserClassLoader(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassPrivate() throws Exception {
        checkNonDefaultClass(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassIsolated() throws Exception {
        checkNonDefaultClass(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassContinuous() throws Exception {
        checkNonDefaultClass(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassShared() throws Exception {
        checkNonDefaultClass(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNamePrivate() throws Exception {
        checkNonDefaultName(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameIsolated() throws Exception {
        checkNonDefaultName(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameContinuous() throws Exception {
        checkNonDefaultName(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameShared() throws Exception {
        checkNonDefaultName(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionPrivate() throws Exception {
        checkDefaultException(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionIsolated() throws Exception {
        checkDefaultException(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionContinuous() throws Exception {
        checkDefaultException(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionShared() throws Exception {
        checkDefaultException(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourcePrivate() throws Exception {
        checkDefaultResource(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceIsolated() throws Exception {
        checkDefaultResource(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceContinuous() throws Exception {
        checkDefaultResource(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceShared() throws Exception {
        checkDefaultResource(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourcePrivate() throws Exception {
        checkNonDefaultClassResource(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceIsolated() throws Exception {
        checkNonDefaultClassResource(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceContinuous() throws Exception {
        checkNonDefaultClassResource(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceShared() throws Exception {
        checkNonDefaultClassResource(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourcePrivate() throws Exception {
        checkNonDefaultNameResource(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceIsolated() throws Exception {
        checkNonDefaultNameResource(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceContinuous() throws Exception {
        checkNonDefaultNameResource(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceShared() throws Exception {
        checkNonDefaultNameResource(DeploymentMode.SHARED);
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefault(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task in Deployment Mode : " + depMode);

        startGrid();

        try {
            int res;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget)targetObj).gridifyDefault("1");
            else
                res = ((TestAopTargetInterface) targetObj).gridifyDefault("1");

            assert res == 1 : "Invalid gridifyDefault result: " + res;
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefaultWithUserClassLoader(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task  in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = targetWithUserClassLoader();

            Method gridifyMtd = targetObj.getClass().getDeclaredMethod("gridifyDefault", String.class);

            res = (Integer) gridifyMtd.invoke(targetObj, "1");

            if (res != 1)
                fail("Method gridifyDefault returned wrong value [result=" + res + ", expect=1]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyDefault(1) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkSingleDeploymentWithUserClassLoader(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        // Create remote grid to execute test on.
        Ignite locIgnite = startGrid();

        Ignite rmtIgnite = startGrid(getTestGridName() + "Remote");

        try {
            AtomicInteger locDepCnt = new AtomicInteger(0);
            AtomicInteger rmtDepCnt = new AtomicInteger(0);

            locIgnite.events().localListen(new TestEventListener(locDepCnt), EVT_TASK_DEPLOYED, EVT_CLASS_DEPLOYED);
            rmtIgnite.events().localListen(new TestEventListener(rmtDepCnt), EVT_TASK_DEPLOYED, EVT_CLASS_DEPLOYED);

            assertEquals(2, locIgnite.cluster().forPredicate(F.<ClusterNode>alwaysTrue()).nodes().size());

            Object targetObj = targetWithUserClassLoader();

            Method gridifyMtd = targetObj.getClass().getDeclaredMethod("gridifyDefault", String.class);

            info("First invocation.");

            int res = (Integer)gridifyMtd.invoke(targetObj, "1");

            assert res == 1 : "Method gridifyDefault returned wrong value [result=" + res + ", expected=1]";

            info("Second invocation.");

            res = (Integer)gridifyMtd.invoke(targetObj, "1");

            assert res == 1 : "Method gridifyDefault returned wrong value [result=" + res + ", expected=1]";

            assert locDepCnt.get() == 1 : "Invalid local deployment count [expected=1, got=" + locDepCnt.get() + ']';
            assert rmtDepCnt.get() == 1 : "Invalid remote deployment count [expected=1, got=" + rmtDepCnt.get() + ']';
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefaultResourceWithUserClassLoader(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task.");

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = targetWithUserClassLoader();

            ClassLoader cl = Thread.currentThread().getContextClassLoader();

            // Set context classloader as user class loader.
            Thread.currentThread().setContextClassLoader(targetObj.getClass().getClassLoader());

            Method gridifyMtd = targetObj.getClass().getDeclaredMethod("gridifyDefaultResource", String.class);

            res = (Integer) gridifyMtd.invoke(targetObj, "2");

            if (res != 2)
                fail("Method gridifyDefaultResource returned wrong value [result=" + res + ", expect=2]");

            // Set old classloader back.
            Thread.currentThread().setContextClassLoader(cl);
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyDefaultResource(2) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultClass(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget) targetObj).gridifyNonDefaultClass("1");
            else
                res = ((TestAopTargetInterface) targetObj).gridifyNonDefaultClass("1");

            if (res != 10)
                fail("Method gridifyNonDefault returned wrong value [result=" + res + ", expect=10]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyNonDefaultClass(0) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultName(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget) targetObj).gridifyNonDefaultName("2");
            else
                res = ((TestAopTargetInterface) targetObj).gridifyNonDefaultName("2");

            if (res != 20)
                fail("Method gridifyNonDefault returned wrong value [result=" + res + ", expect=2]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyNonDefaultName(2) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void checkDefaultException(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task and exception in Deployment Mode : " + depMode);

        startGrid();

        try {
            Object targetObj = target();

            boolean isE = false;

            try {
                if (targetObj instanceof TestAopTarget)
                    ((TestAopTarget) targetObj).gridifyDefaultException("1");
                else
                    ((TestAopTargetInterface) targetObj).gridifyDefaultException("1");
            }
            catch (TestGridifyException e) {
                info("@Gridify method gridifyDefaultException(0) returns exception: " + e);

                isE = true;
            }
            catch (Exception e) {
                e.printStackTrace();

                fail("@Gridify method gridifyDefaultException(0) returns exception [exception" + e
                    + ", expect=" + TestGridifyException.class.getName() + ']');
            }

            if (!isE)
                fail("@Gridify method gridifyDefaultException(0) does not return any exception.");
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefaultResource(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task in Deploy Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget)targetObj).gridifyDefaultResource("1");
            else
                res = ((TestAopTargetInterface)targetObj).gridifyDefaultResource("1");

            if (res != 1)
                fail("Method gridifyDefaultResource returned wrong value [result=" + res + ", expect=1]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyDefaultResource(0) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultClassResource(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deploy Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget) targetObj).gridifyNonDefaultClassResource("3");
            else
                res = ((TestAopTargetInterface) targetObj).gridifyNonDefaultClassResource("3");

            if (res != 30)
                fail("Method gridifyNonDefaultClassResource returned wrong value [result=" + res + ", expect=3]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyNonDefaultClassResource(3) [result=" + res + ']');
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultNameResource(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof TestAopTarget)
                res = ((TestAopTarget)targetObj).gridifyNonDefaultNameResource("4");
            else
                res = ((TestAopTargetInterface)targetObj).gridifyNonDefaultNameResource("4");

            if (res != 40)
                fail("Method gridifyNonDefaultNameResource returned wrong value [result=" + res + ", expect=4]");
        }
        finally {
            stopGrid();
        }

        info("Executed @Gridify method gridifyNonDefaultNameResource(4) [result=" + res + ']');
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentSpi(new LocalDeploymentSpi());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @return Test target.
     */
    protected abstract Object target();

    /**
     * @return Target.
     * @throws Exception If failed.
     */
    protected Object targetWithUserClassLoader() throws Exception {
        // Notice that we use another resource naming because file has path.
        ClassLoader tstClsLdr = new GridTestClassLoader(
            Collections.singletonMap("org/apache/ignite/gridify/test_resource.properties", "param1=2"),
            getClass().getClassLoader(),
            TestAopTarget.class.getName(), TestAopTargetInterface.class.getName());

        return tstClsLdr.loadClass("org.apache.ignite.gridify.TestAopTarget").newInstance();
    }

    /**
     * Event listener.
     */
    private static final class TestEventListener implements IgnitePredicate<Event> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Counter. */
        private final AtomicInteger cnt;

        /**
         * @param cnt Deploy counter.
         */
        private TestEventListener(AtomicInteger cnt) { this.cnt = cnt; }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            if ((evt.type() == EVT_TASK_DEPLOYED || evt.type() == EVT_CLASS_DEPLOYED) &&
                evt.message() != null && evt.message().contains("TestAopTarget"))
                cnt.addAndGet(1);

            return true;
        }
    }
}
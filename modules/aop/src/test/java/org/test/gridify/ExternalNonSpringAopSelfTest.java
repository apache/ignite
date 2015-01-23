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

package org.test.gridify;

import org.apache.ignite.gridify.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.deployment.local.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * To run this test with JBoss AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters to enable jboss online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      -Djboss.aop.class.path=[path to grid compiled classes (Idea out folder) or path to gridgain.jar]
 *      -Djboss.aop.exclude=org,com -Djboss.aop.include=org.gridgain
 *
 * 2. The following jars should be in a classpath:
 *      ${GRIDGAIN_HOME}libs/javassist-3.x.x.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aspect-library-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-common-4.2.2.jar
 *      ${GRIDGAIN_HOME}libs/trove-1.0.2.jar
 *
 * To run this test with AspectJ AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters for enable AspectJ online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}/libs/aspectjweaver-1.7.2.jar
 *
 * 2. Classpath should contains the ${GRIDGAIN_HOME}/modules/tests/config/aop/aspectj folder.
 */
@GridCommonTest(group="AOP")
public class ExternalNonSpringAopSelfTest extends GridCommonAbstractTest {
    /** */
    private IgniteDeploymentMode depMode = IgniteDeploymentMode.PRIVATE;

    /** */
    public ExternalNonSpringAopSelfTest() {
        super(/**start grid*/false);
    }

    /**
     * @return External AOP target.
     */
    protected ExternalAopTarget getTarget() {
        return new ExternalAopTarget();
    }

    /**
     * @throws Exception If failed.
     */
    private void deployTask() throws Exception {
        G.ignite(getTestGridName()).compute().localDeployTask(GridTestGridifyTask.class,
            GridTestGridifyTask.class.getClassLoader());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultPrivate() throws Exception {
        checkDefault(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultIsolated() throws Exception {
        checkDefault(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultContinuous() throws Exception {
        checkDefault(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultShared() throws Exception {
        checkDefault(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassPrivate() throws Exception {
        checkNonDefaultClass(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassIsolated() throws Exception {
        checkNonDefaultClass(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultContinuous() throws Exception {
        checkNonDefaultClass(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultShared() throws Exception {
        checkNonDefaultClass(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNamePrivate() throws Exception {
        checkNonDefaultName(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameIsolated() throws Exception {
        checkNonDefaultName(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameContinuous() throws Exception {
        checkNonDefaultName(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameShared() throws Exception {
        checkNonDefaultName(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassPrivate() throws Exception {
        checkTaskNameAndTaskClass(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassIsolated() throws Exception {
        checkTaskNameAndTaskClass(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassContinuous() throws Exception {
        checkTaskNameAndTaskClass(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassShared() throws Exception {
        checkTaskNameAndTaskClass(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionPrivate() throws Exception {
        checkDefaultException(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionIsolated() throws Exception {
        checkDefaultException(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionContinuous() throws Exception {
        checkDefaultException(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionShared() throws Exception {
        checkDefaultException(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourcePrivate() throws Exception {
        checkDefaultResource(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceIsolated() throws Exception {
        checkDefaultResource(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceContinuous() throws Exception {
        checkDefaultResource(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceShared() throws Exception {
        checkDefaultResource(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourcePrivate() throws Exception {
        checkNonDefaultClassResource(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceIsolated() throws Exception {
        checkNonDefaultClassResource(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceContinuous() throws Exception {
        checkNonDefaultClassResource(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceShared() throws Exception {
        checkNonDefaultClassResource(IgniteDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourcePrivate() throws Exception {
        checkNonDefaultNameResource(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceIsolated() throws Exception {
        checkNonDefaultNameResource(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceContinuous() throws Exception {
        checkNonDefaultNameResource(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceShared() throws Exception {
        checkNonDefaultNameResource(IgniteDeploymentMode.SHARED);
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefault(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            info("Start Gridify test with Default AOP Task in Deployment Mode : " + depMode);

            startGrid();

            deployTask();

            int res = getTarget().gridifyDefault("1");

            if (res != 1)
                fail("Method gridifyDefault returns wrong value [result=" + res + ", expect=1]");

            info("Executed @Gridify method gridifyDefault(1) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultClass(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

            startGrid();

            deployTask();

            int res = getTarget().gridifyNonDefaultClass("1");

            if (res != 10)
                fail("Method gridifyNonDefault returns wrong value [result=" + res + ", expect=1]");

            info("Executed @Gridify method gridifyNonDefaultClass(0) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultName(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

            startGrid();

            deployTask();

            int res = getTarget().gridifyNonDefaultName("2");

            if (res != 20)
                fail("Method gridifyNonDefault returns wrong value [result=" + res + ", expect=2]");

            info("Executed @Gridify method gridifyNonDefaultName(0) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void checkTaskNameAndTaskClass(IgniteDeploymentMode depMode) {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        try {
            startGrid();

            deployTask();

            getTarget().gridifyTaskClassAndTaskName("3");

            assert false : "No exception thrown";
        }
        catch (Exception e) {
            info("Got expected exception: " + e);
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void checkDefaultException(IgniteDeploymentMode depMode) {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task and exception in Deployment Mode : " + depMode);

        boolean isException = false;

        try {
            startGrid();

            deployTask();

            getTarget().gridifyDefaultException("0");
        }
        catch (ExternalGridifyException e) {
            info("@Gridify method gridifyDefaultException(0) returns exception: " + e);

            isException = true;
        }
        catch (Exception e) {
            e.printStackTrace();

            fail("@Gridify method gridifyDefaultException(0) returns exception [exception" + e +
                ", expect=" + GridTestGridifyException.class.getName() + ']');
        }
        finally {
            stopGrid();
        }

        if (isException == false)
            fail("@Gridify method gridifyDefaultException(0) does not return any exception.");
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefaultResource(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task in Deployment Mode : " + depMode);

        try {
            startGrid();

            deployTask();

            int res = getTarget().gridifyDefaultResource("0");

            if (res != 0)
                fail("Method gridifyDefaultResource returns wrong value [result=" + res + ", expect=0]");

            info("Executed @Gridify method gridifyDefaultResource(0) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultClassResource(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        try {
            startGrid();

            deployTask();

            int res = getTarget().gridifyNonDefaultClassResource("3");

            if (res != 30)
                fail("Method gridifyNonDefaultClassResource returns wrong value [result=" + res + ", expect=3]");

            info("Executed @Gridify method gridifyNonDefaultClassResource(3) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkNonDefaultNameResource(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        try {
            startGrid();

            deployTask();

            int res = getTarget().gridifyNonDefaultNameResource("4");

            if (res != 40)
                fail("Method gridifyNonDefaultNameResource returns wrong value [result=" + res + ", expect=4]");

            info("Executed @Gridify method gridifyNonDefaultNameResource(4) [result=" + res + ']');
        }
        finally {
            stopGrid();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();
        cfg.setDeploymentSpi(new LocalDeploymentSpi());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        cfg.setDeploymentMode(depMode);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @return Test grid name.
     */
    @Override public String getTestGridName() {
        return "ExternalAopTarget";
    }
}

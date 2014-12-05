/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.test.gridify;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.gridify.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

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
public class GridExternalNonSpringAopSelfTest extends GridCommonAbstractTest {
    /** */
    private GridDeploymentMode depMode = GridDeploymentMode.PRIVATE;

    /** */
    public GridExternalNonSpringAopSelfTest() {
        super(/**start grid*/false);
    }

    /**
     * @return External AOP target.
     */
    protected GridExternalAopTarget getTarget() {
        return new GridExternalAopTarget();
    }

    /**
     * @throws Exception If failed.
     */
    private void deployTask() throws Exception {
        G.grid(getTestGridName()).compute().localDeployTask(GridTestGridifyTask.class,
            GridTestGridifyTask.class.getClassLoader());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultPrivate() throws Exception {
        checkDefault(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultIsolated() throws Exception {
        checkDefault(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultContinuous() throws Exception {
        checkDefault(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultShared() throws Exception {
        checkDefault(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassPrivate() throws Exception {
        checkNonDefaultClass(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassIsolated() throws Exception {
        checkNonDefaultClass(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultContinuous() throws Exception {
        checkNonDefaultClass(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultShared() throws Exception {
        checkNonDefaultClass(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNamePrivate() throws Exception {
        checkNonDefaultName(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameIsolated() throws Exception {
        checkNonDefaultName(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameContinuous() throws Exception {
        checkNonDefaultName(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameShared() throws Exception {
        checkNonDefaultName(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassPrivate() throws Exception {
        checkTaskNameAndTaskClass(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassIsolated() throws Exception {
        checkTaskNameAndTaskClass(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassContinuous() throws Exception {
        checkTaskNameAndTaskClass(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskNameAndTaskClassShared() throws Exception {
        checkTaskNameAndTaskClass(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionPrivate() throws Exception {
        checkDefaultException(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionIsolated() throws Exception {
        checkDefaultException(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionContinuous() throws Exception {
        checkDefaultException(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultExceptionShared() throws Exception {
        checkDefaultException(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourcePrivate() throws Exception {
        checkDefaultResource(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceIsolated() throws Exception {
        checkDefaultResource(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceContinuous() throws Exception {
        checkDefaultResource(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceShared() throws Exception {
        checkDefaultResource(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourcePrivate() throws Exception {
        checkNonDefaultClassResource(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceIsolated() throws Exception {
        checkNonDefaultClassResource(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceContinuous() throws Exception {
        checkNonDefaultClassResource(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassResourceShared() throws Exception {
        checkNonDefaultClassResource(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourcePrivate() throws Exception {
        checkNonDefaultNameResource(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceIsolated() throws Exception {
        checkNonDefaultNameResource(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceContinuous() throws Exception {
        checkNonDefaultNameResource(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultNameResourceShared() throws Exception {
        checkNonDefaultNameResource(GridDeploymentMode.SHARED);
    }

    /**
     * @param depMode Deployment mode to use.
     * @throws Exception If failed.
     */
    private void checkDefault(GridDeploymentMode depMode) throws Exception {
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
    private void checkNonDefaultClass(GridDeploymentMode depMode) throws Exception {
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
    private void checkNonDefaultName(GridDeploymentMode depMode) throws Exception {
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
    private void checkTaskNameAndTaskClass(GridDeploymentMode depMode) {
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
    private void checkDefaultException(GridDeploymentMode depMode) {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task and exception in Deployment Mode : " + depMode);

        boolean isException = false;

        try {
            startGrid();

            deployTask();

            getTarget().gridifyDefaultException("0");
        }
        catch (GridExternalGridifyException e) {
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
    private void checkDefaultResource(GridDeploymentMode depMode) throws Exception {
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
    private void checkNonDefaultClassResource(GridDeploymentMode depMode) throws Exception {
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
    private void checkNonDefaultNameResource(GridDeploymentMode depMode) throws Exception {
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
        cfg.setDeploymentSpi(new GridLocalDeploymentSpi());

        ((GridTcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        cfg.setDeploymentMode(depMode);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @return Test grid name.
     */
    @Override public String getTestGridName() {
        return "GridExternalAopTarget";
    }
}

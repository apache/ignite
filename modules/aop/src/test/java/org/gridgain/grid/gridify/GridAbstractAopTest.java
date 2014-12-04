/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Abstract AOP test.
 */
@SuppressWarnings( {"OverlyStrongTypeCast", "JUnitAbstractTestClassNamingConvention", "ProhibitedExceptionDeclared", "IfMayBeConditional"})
public abstract class GridAbstractAopTest extends GridCommonAbstractTest {
    /** */
    private GridDeploymentMode depMode = GridDeploymentMode.PRIVATE;

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
    public void testDefaultWithUserClassLoaderPrivate() throws Exception {
        checkDefaultWithUserClassLoader(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderIsolated() throws Exception {
        checkDefaultWithUserClassLoader(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderContinuous() throws Exception {
        checkDefaultWithUserClassLoader(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultWithUserClassLoaderShared() throws Exception {
        checkDefaultWithUserClassLoader(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderPrivate() throws Exception {
        checkSingleDeploymentWithUserClassLoader(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderIsolated() throws Exception {
        checkSingleDeploymentWithUserClassLoader(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderContinuous() throws Exception {
        checkSingleDeploymentWithUserClassLoader(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSingleDeploymentWithUserClassLoaderShared() throws Exception {
        checkSingleDeploymentWithUserClassLoader(GridDeploymentMode.SHARED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderPrivate() throws Exception {
        checkDefaultResourceWithUserClassLoader(GridDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderIsolated() throws Exception {
        checkDefaultResourceWithUserClassLoader(GridDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderContinuous() throws Exception {
        checkDefaultResourceWithUserClassLoader(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDefaultResourceWithUserClassLoaderShared() throws Exception {
        checkDefaultResourceWithUserClassLoader(GridDeploymentMode.SHARED);
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
    public void testNonDefaultClassContinuous() throws Exception {
        checkNonDefaultClass(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNonDefaultClassShared() throws Exception {
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
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task in Deployment Mode : " + depMode);

        startGrid();

        try {
            int res;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget)targetObj).gridifyDefault("1");
            else
                res = ((GridTestAopTargetInterface) targetObj).gridifyDefault("1");

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
    private void checkDefaultWithUserClassLoader(GridDeploymentMode depMode) throws Exception {
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
    private void checkSingleDeploymentWithUserClassLoader(GridDeploymentMode depMode) throws Exception {
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
    private void checkDefaultResourceWithUserClassLoader(GridDeploymentMode depMode) throws Exception {
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
    private void checkNonDefaultClass(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget) targetObj).gridifyNonDefaultClass("1");
            else
                res = ((GridTestAopTargetInterface) targetObj).gridifyNonDefaultClass("1");

            if (res != 10)
                fail("Method gridifyNonDefault returned wrong value [result=" + res + ", expect=1]");
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
    private void checkNonDefaultName(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget) targetObj).gridifyNonDefaultName("2");
            else
                res = ((GridTestAopTargetInterface) targetObj).gridifyNonDefaultName("2");

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
    private void checkDefaultException(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task and exception in Deployment Mode : " + depMode);

        startGrid();

        try {
            Object targetObj = target();

            boolean isE = false;

            try {
                if (targetObj instanceof GridTestAopTarget)
                    ((GridTestAopTarget) targetObj).gridifyDefaultException("1");
                else
                    ((GridTestAopTargetInterface) targetObj).gridifyDefaultException("1");
            }
            catch (GridTestGridifyException e) {
                info("@Gridify method gridifyDefaultException(0) returns exception: " + e);

                isE = true;
            }
            catch (Exception e) {
                e.printStackTrace();

                fail("@Gridify method gridifyDefaultException(0) returns exception [exception" + e
                    + ", expect=" + GridTestGridifyException.class.getName() + ']');
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
    private void checkDefaultResource(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Default AOP Task in Deploy Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget)targetObj).gridifyDefaultResource("1");
            else
                res = ((GridTestAopTargetInterface)targetObj).gridifyDefaultResource("1");

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
    private void checkNonDefaultClassResource(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deploy Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget) targetObj).gridifyNonDefaultClassResource("3");
            else
                res = ((GridTestAopTargetInterface) targetObj).gridifyNonDefaultClassResource("3");

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
    private void checkNonDefaultNameResource(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        info("Start Gridify test with Test AOP Task in Deployment Mode : " + depMode);

        startGrid();

        int res;

        try {
            res = -1;

            Object targetObj = target();

            if (targetObj instanceof GridTestAopTarget)
                res = ((GridTestAopTarget)targetObj).gridifyNonDefaultNameResource("4");
            else
                res = ((GridTestAopTargetInterface)targetObj).gridifyNonDefaultNameResource("4");

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

        cfg.setDeploymentSpi(new GridLocalDeploymentSpi());

        ((GridTcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

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
            Collections.singletonMap("org/gridgain/grid/gridify/test_resource.properties", "param1=2"),
            getClass().getClassLoader(),
            GridTestAopTarget.class.getName(), GridTestAopTargetInterface.class.getName());

        return tstClsLdr.loadClass("org.gridgain.grid.gridify.GridTestAopTarget").newInstance();
    }

    /**
     * Event listener.
     */
    private static final class TestEventListener implements GridPredicate<GridEvent> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Counter. */
        private final AtomicInteger cnt;

        /**
         * @param cnt Deploy counter.
         */
        private TestEventListener(AtomicInteger cnt) { this.cnt = cnt; }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            if ((evt.type() == EVT_TASK_DEPLOYED || evt.type() == EVT_CLASS_DEPLOYED) &&
                evt.message() != null && !evt.message().contains("GridTopic"))
                cnt.addAndGet(1);

            return true;
        }
    }
}

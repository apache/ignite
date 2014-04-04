/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p.jboss;

import junit.framework.*;
import org.gridgain.grid.logger.jboss.*;
import org.gridgain.grid.p2p.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.jboss.system.*;
import java.util.*;

/**
 * JBoss P2P test.
 *
 * Use the following descriptor for JBoss
 * ${GRIDGAIN_HOME}/modules/tests/config/jboss/p2p/jboss-services.xml
 *
 * NOTE: add Jvm option '-DGRIDGAIN_TEST_PROP_DISABLE_LOG4J=true' in JBoss start script
 * to disable logger from junit tests module. By default
 * {@link GridTestProperties} load log4j configuration.
 */
@SuppressWarnings({"UnusedCatchParameter", "JUnitTestCaseWithNoTests", "ProhibitedExceptionDeclared"})
public class GridJbossP2PTest extends ServiceMBeanSupport implements GridJbossP2PTestMBean {
    /** */
    private GridTestResources res;

    /** {@inheritDoc} */
    @Override protected void startService() throws Exception {
        // Initialize test resources.
        res = new GridTestResources(new GridJbossLogger(getLog()));

        if (getLog().isInfoEnabled()) {
            getLog().info("Jboss test started.");
        }
    }

    /** {@inheritDoc} */
    @Override protected void stopService() throws Exception {
        // Clean test resources.
        res = null;

        if (getLog().isInfoEnabled()) {
            getLog().info("Jboss test stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public void doubleDeploymentSelfTest() {
        runTest("GridP2PDoubleDeploymentSelfTest", new GridP2PDoubleDeploymentSelfTest() {
            /** {@inheritDoc} */
            @Override protected GridTestResources getTestResources() { return res; }

            /** {@inheritDoc} */
            @Override protected void runTest() throws Throwable {
                testPrivateMode();
                testIsolatedMode();
                testContinuousMode();
                testSharedMode();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void hotRedeploymentSelfTest() {
        runTest("GridP2PHotRedeploymentSelfTest", new GridP2PHotRedeploymentSelfTest() {
            /** {@inheritDoc} */
            @Override protected GridTestResources getTestResources() { return res; }

            /** {@inheritDoc} */
            @Override protected void runTest() throws Throwable {
                testSameClassLoaderIsolatedMode();
                testSameClassLoaderIsolatedClassLoaderMode();
                testSameClassLoaderContinuousMode();
                testSameClassLoaderSharedMode();

                testNewClassLoaderHotRedeploymentPrivateMode();
                testNewClassLoaderHotRedeploymentIsolatedMode();
                testNewClassLoaderHotRedeploymentContinuousMode();
                testNewClassLoaderHotRedeploymentSharedMode();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void uploadSelfTest() {
        runTest("GridP2PSameClassLoaderSelfTest", new GridP2PSameClassLoaderSelfTest() {
            /** {@inheritDoc} */
            @Override protected GridTestResources getTestResources() { return res; }

            /** {@inheritDoc} */
            @Override protected void runTest() throws Throwable {
                testPrivateMode();
                testIsolatedMode();
                testContinuousMode();
                testSharedMode();
            }
        });
    }

    /**
     * @param name Test name.
     * @param test Test.
     */
    private void runTest(String name, Test test) {
        assert name != null;
        assert test != null;

        TestResult res = new TestResult();

        TestSuite suite = new TestSuite();

        suite.runTest(test, res);

        printTestResult(name, res);
    }

    /**
     * @param name Test name.
     * @param res Result.
     */
    @SuppressWarnings({"HardcodedLineSeparator", "unchecked"})
    private void printTestResult(String name, TestResult res) {
        StringBuilder builder = new StringBuilder();

        builder.append("Test result [test=").append(name).append('\n').
            append("wasSuccessful=").append(res.wasSuccessful()).append(", ").
            append("errorCount=").append(res.errorCount()).append(", ").
            append("failureCount=").append(res.failureCount()).append(", ");

        if (res.failureCount() > 0) {
            builder.append("failures=[");

            prepareFailures(builder, res.failures());

            builder.append("], ");
        }

        if (res.errorCount() > 0) {
            builder.append("errors=[");

            prepareFailures(builder, res.errors());

            builder.append("], ");
        }

        builder.append(res.toString()).append("]\n");

        System.out.println(builder.toString());
    }

    /**
     * @param builder String builder.
     * @param failures Enumerated test failures.
     */
    @SuppressWarnings({"HardcodedLineSeparator"})
    private void prepareFailures(StringBuilder builder, Enumeration<TestFailure> failures) {
        while (failures.hasMoreElements()) {
            Object obj = failures.nextElement();

            if (obj instanceof TestFailure) {
                TestFailure failure = (TestFailure)obj;

                builder.append("message=").append(failure.exceptionMessage());
                builder.append(", exception=").append(failure.thrownException());
                builder.append('\n');
            }
            else {
                builder.append(obj.toString());
            }

            if (failures.hasMoreElements()) {
                builder.append(", ");
            }
        }
    }
}

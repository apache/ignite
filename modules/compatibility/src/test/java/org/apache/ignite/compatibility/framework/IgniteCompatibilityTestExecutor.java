/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.framework;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp;
import org.junit.internal.runners.statements.InvokeMethod;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import sun.jvmstat.monitor.HostIdentifier;
import sun.jvmstat.monitor.MonitoredHost;
import sun.jvmstat.monitor.MonitoredVm;
import sun.jvmstat.monitor.MonitoredVmUtil;
import sun.jvmstat.monitor.VmIdentifier;

/**
 *
 */
public class IgniteCompatibilityTestExecutor extends Statement {
    /** */
    private static final Logger log = Logger.getLogger(IgniteCompatibilityTestExecutor.class.getName());

    /** */
    private final Statement origStmt;

    /** */
    private final long timeout;

    /**
     * @param origStmt Original statement.
     * @param timeout Test timeout.
     */
    public IgniteCompatibilityTestExecutor(Statement origStmt, long timeout) {
        this.origStmt = origStmt;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public void evaluate() throws Throwable {
        killAll();

        IgniteCompatibilityRemoteNode.startTest();

        try {
            TestRunnerThread thread = new TestRunnerThread(origStmt);

            thread.start();

            thread.join(timeout);

            if (thread.isAlive()) {
                log.log(Level.SEVERE, "Test has been timed out and will be stopped, debug info will be dumped before interruption");

                dumpDebugInfo();

                IgniteCompatibilityRemoteNode.stopTest();

                stopAllNodes();

                thread.interrupt();

                thread.join();

                throw new Exception("Test timed out after " + timeout + " milliseconds");
            }
            else {
                if (thread.err != null) {
                    if (thread.err instanceof TimeoutException ||
                        thread.err instanceof IgniteFutureTimeoutException ||
                        thread.err instanceof IgniteFutureTimeoutCheckedException) {
                        log.log(Level.SEVERE, "Test failed with timeout error, try dump debug info");

                        dumpDebugInfo();
                    }

                    throw thread.err;
                }
            }
        }
        finally {
            IgniteCompatibilityRemoteNode.stopTest();

            stopAllNodes();

            killAll();
        }
    }

    /**
     *
     */
    private static void dumpDebugInfo() {
        IgniteCompatibilityRemoteNode.dumpDebugInfoForStarted();

        List<Ignite> locNodes = G.allGrids();

        if (!locNodes.isEmpty()) {
            log.log(Level.SEVERE, "Dump debug info for local nodes:");

            for (Ignite node : G.allGrids())
                ((IgniteKernal)node).dumpDebugInfo();
        }
    }

    /** */
    private static final Collection<String> TEST_APPS = IgniteCompatibilityTestConfig.get().testApps();

    /**
     * Kill all Jvm run by {#link IgniteCompatibilityRemoteNodeStartApp}. Works based on jps command.
     */
    private static void killAll() {
        Set<Integer> jvms;
        MonitoredHost monitoredHost;

        try {
            monitoredHost = MonitoredHost.getMonitoredHost(new HostIdentifier("localhost"));

            jvms = monitoredHost.activeVms();
        }
        catch (Exception e) {
            log.log(Level.SEVERE, "Failed to get running JVMs", e);

            return;
        }

        int locPid = U.jvmPid();

        for (Integer jvmId : jvms) {
            if (locPid == jvmId)
                continue;

            String manCls = null;

            try {
                MonitoredVm vm = monitoredHost.getMonitoredVm(new VmIdentifier("//" + jvmId + "?mode=r"), 0);

                manCls = MonitoredVmUtil.mainClass(vm, true);

                if (TEST_APPS.contains(manCls)) {
                    log.info("Kill test JVM, class: " + manCls);

                    Process killProc = Runtime.getRuntime().exec(U.isWindows() ?
                            new String[] {"taskkill", "/pid", jvmId.toString(), "/f", "/t"} :
                            new String[] {"kill", "-9", jvmId.toString()});

                    killProc.waitFor();
                }
            }
            catch (Exception e) {
                if (!e.getMessage().equals(jvmId + " not found"))
                    log.log(Level.SEVERE, "Could not kill test java processes [cls=" + manCls + ", pid = " + jvmId, e);
            }
        }
    }

    /**
     *
     */
    private static void stopAllNodes() {
        G.stopAll(true);
    }

    /**
     *
     */
    private static class TestRunnerThread extends Thread {
        /** */
        private final Statement stmt;

        /** */
        private Throwable err;

        /**
         * @param stmt Statement to execute.
         */
        TestRunnerThread(Statement stmt) {
            this.stmt = stmt;

            setName("test-runner");
        }

        /** {@inheritDoc} */
        @Override public void run() {
            String testName = null;

            try {
                Object stmt0 = stmt;

                if (stmt instanceof RunBefores)
                    stmt0 = GridTestUtils.getFieldValue(stmt, "fNext");

                if (stmt0 instanceof InvokeMethod) {
                    FrameworkMethod m = GridTestUtils.getFieldValue(stmt0, "fTestMethod");
                    Object test = GridTestUtils.getFieldValue(stmt0, "fTarget");

                    testName = test.getClass().getName() + "." + m.getMethod().getName();

                    log.info("Run test: " + testName);
                }
            }
            catch (Throwable e) {
                log.warning("Failed to resolve test name: " + e);
            }

            try {
                stmt.evaluate();
            }
            catch (Throwable e) {
                this.err = e;
            }
            finally {
                log.info("Test finished: " + testName);

                if (err != null)
                    log.info("Test failed with error [test=" + testName + ", err=" + err + ']');
            }
        }
    }
}

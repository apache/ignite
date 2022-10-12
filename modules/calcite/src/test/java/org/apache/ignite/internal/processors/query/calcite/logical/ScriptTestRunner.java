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

package org.apache.ignite.internal.processors.query.calcite.logical;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.thread.IgniteThread;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

/**
 *
 */
public class ScriptTestRunner extends Runner {
    /** Filesystem. */
    private static final FileSystem FS = FileSystems.getDefault();

    /** Shared finder. */
    private static final TcpDiscoveryVmIpFinder sharedFinder = new TcpDiscoveryVmIpFinder().setShared(true);

    /** */
    private static IgniteLogger log;

    static {
        try {
            log = new GridTestLog4jLogger(U.resolveIgnitePath("modules/core/src/test/config/log4j2-test.xml"));
        }
        catch (Exception e) {
            e.printStackTrace(System.err);

            log = null;

            assert false : "Cannot init logger";
        }
    }

    /** */
    private final Class<?> testCls;

    /** Scripts root directory. */
    private final Path scriptsRoot;

    /** Regex to filter test path to run only specified tests. */
    private final Pattern testRegex;

    /** Nodes count. */
    private final int nodes;

    /** Restart cluster for each test group. */
    private final boolean restartCluster;

    /** Test script timeout. */
    private final long timeout;

    /** */
    public ScriptTestRunner(Class<?> testCls) {
        this.testCls = testCls;
        ScriptRunnerTestsEnvironment env = testCls.getAnnotation(ScriptRunnerTestsEnvironment.class);

        assert !F.isEmpty(env.scriptsRoot());

        nodes = env.nodes();
        scriptsRoot = FS.getPath(U.resolveIgnitePath(env.scriptsRoot()).getPath());
        testRegex = F.isEmpty(env.regex()) ? null : Pattern.compile(env.regex());
        restartCluster = env.restart();
        timeout = env.timeout();
    }

    /** {@inheritDoc} */
    @Override public Description getDescription() {
        return Description.createSuiteDescription(testCls.getName(), "scripts");
    }

    /** {@inheritDoc} */
    @Override public void run(final RunNotifier notifier) {
        try {
            Files.walk(scriptsRoot).sorted().forEach((p) -> {
                if (p.equals(scriptsRoot))
                    return;

                if (Files.isDirectory(p)) {
                    if (!F.isEmpty(Ignition.allGrids()) && restartCluster) {
                        log.info(">>> Restart cluster");

                        Ignition.stopAll(false);
                    }

                    return;
                }

                runTest(p, notifier);
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            Ignition.stopAll(false);
        }
    }

    /** */
    private void runTest(Path test, RunNotifier notifier) {
        String dirName;
        if (test.getNameCount() - 1 > scriptsRoot.getNameCount())
            dirName = test.subpath(scriptsRoot.getNameCount(), test.getNameCount() - 1).toString();
        else
            dirName = scriptsRoot.subpath(scriptsRoot.getNameCount() - 1, scriptsRoot.getNameCount()).toString();

        String fileName = test.getFileName().toString();

        Description desc = Description.createTestDescription(dirName, fileName);

        if (testRegex != null && !testRegex.matcher(test.toString()).find())
            return;

        if (testRegex == null && !fileName.endsWith(".test") && !fileName.endsWith(".test_slow")) {
            if (fileName.endsWith(".test_ignore"))
                notifier.fireTestIgnored(desc);

            return;
        }

        beforeTest();

        notifier.fireTestStarted(desc);

        try {
            Ignite ign = F.first(Ignition.allGrids());

            QueryEngine engine = Commons.lookupComponent(
                ((IgniteEx)ign).context(),
                QueryEngine.class
            );

            SqlScriptRunner scriptTestRunner = new SqlScriptRunner(test, engine, log);

            log.info(">>> Start: " + dirName + "/" + fileName);

            runScript(scriptTestRunner);
        }
        catch (Throwable e) {
            notifier.fireTestFailure(new Failure(desc, e));
        }
        finally {
            log.info(">>> Finish: " + dirName + "/" + fileName);
            notifier.fireTestFinished(desc);
        }
    }

    /** */
    private void beforeTest() {
        if (F.isEmpty(Ignition.allGrids()))
            startCluster();
        else {
            Ignite ign = F.first(Ignition.allGrids());

            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);
        }
    }

    /** */
    private void startCluster() {
        for (int i = 0; i < nodes; ++i) {
            Ignition.start(
                new IgniteConfiguration()
                    .setIgniteInstanceName("srv" + i)
                    .setDiscoverySpi(
                        new TcpDiscoverySpi()
                            .setIpFinder(sharedFinder)
                    )
                    .setGridLogger(log)
            );
        }
    }

    /** */
    private void runScript(SqlScriptRunner scriptRunner) throws Throwable {
        final AtomicReference<Throwable> ex = new AtomicReference<>();

        Thread runner = new IgniteThread("srv0", "test-runner", new Runnable() {
            @Override public void run() {
                try {
                    scriptRunner.run();
                }
                catch (Throwable e) {
                    ex.set(e);
                }
            }
        });

        runner.start();

        runner.join(timeout);

        if (runner.isAlive()) {
            U.error(log,
                "Test has been timed out and will be interrupted");

            List<Ignite> nodes = IgnitionEx.allGridsx();

            for (Ignite node : nodes)
                ((IgniteKernal)node).dumpDebugInfo();

            // We dump threads to stdout, because we can loose logs in case
            // the build is cancelled on TeamCity.
            U.dumpThreads(null);

            U.dumpThreads(log);

            // Try to interrupt runner several times for case when InterruptedException is handled invalid.
            for (int i = 0; i < 100 && runner.isAlive(); ++i) {
                U.interrupt(runner);

                U.sleep(10);
            }

            U.join(runner, log);

            // Restart cluster
            Ignition.stopAll(true);
            startCluster();

            throw new TimeoutException("Test has been timed out");
        }

        Throwable t = ex.get();

        if (t != null)
            throw t;
    }
}

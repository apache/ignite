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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

/**
 *
 */
public class LogicalTestRunner extends Runner {
    /** Filesystem. */
    private static final FileSystem FS = FileSystems.getDefault();

    /** Shared finder. */
    private static final TcpDiscoveryVmIpFinder sharedFinder = new TcpDiscoveryVmIpFinder().setShared(true);

    /** */
    private static IgniteLogger rootLog;

    static {
        try {
            rootLog = new GridTestLog4jLogger("src/test/config/log4j-test.xml");
        }
        catch (Exception e) {
            e.printStackTrace();

            rootLog = null;

            assert false : "Cannot init logger";
        }
    }

    /** */
    private final Class<?> testCls;

    /** Scripts root directory. */
    private final Path scriptsRoot;

    /** Single script to execute. */
    private final Path script;

    /** Nodes count. */
    private final int nodes;

    /** Restart cluster for each test group. */
    private final boolean restartCluster;

    /** */
    public LogicalTestRunner(Class<?> testCls) {
        this.testCls = testCls;
        LogicalTestEnvironment env = testCls.getAnnotation(LogicalTestEnvironment.class);

        assert !F.isEmpty(env.scriptsRoot());

        nodes = env.nodes();
        scriptsRoot = FS.getPath(env.scriptsRoot());
        script = F.isEmpty(env.script()) ? null : FS.getPath(env.script());
        restartCluster = env.restart();
    }

    /** {@inheritDoc} */
    @Override public Description getDescription() {
        return Description.createSuiteDescription(testCls.getName(), "scripts");
    }

    /** {@inheritDoc} */
    @Override public void run(RunNotifier notifier) {
        try {
            if (script == null) {
                Files.walk(scriptsRoot).forEach((p) -> {
                    if (p.equals(scriptsRoot))
                        return;

                    if (Files.isDirectory(p)) {
                        if (!F.isEmpty(Ignition.allGrids()) && restartCluster) {
                            rootLog.info(">>> Restart cluster");

                            Ignition.stopAll(false);
                        }

                        if (F.isEmpty(Ignition.allGrids()))
                            startGrid();

                        return;
                    }

                    String dirName = p.getParent().toString().substring(scriptsRoot.toString().length() + 1);
                    String fileName = p.getFileName().toString();

                    if (!fileName.endsWith("test") && !fileName.endsWith("test_slow"))
                        return;

                    Ignite ign = F.first(Ignition.allGrids());

                    for (String cacheName : ign.cacheNames())
                        ign.destroyCache(cacheName);

                    Description desc = Description.createTestDescription(dirName, fileName);

                    notifier.fireTestStarted(desc);

                    try {
                        QueryEngine engine = Commons.lookupComponent(
                            ((IgniteEx)ign).context(),
                            QueryEngine.class
                        );

                        ScriptTestRunner scriptTestRunner = new ScriptTestRunner(p, engine, rootLog);

                        rootLog.info(">>> Start: " + dirName + "/" + fileName);

                        scriptTestRunner.run();
                    }
                    catch (Throwable e) {
                        notifier.fireTestFailure(new Failure(desc, e));
                    }
                    finally {
                        rootLog.info(">>> Finish: " + dirName + "/" + fileName);
                        notifier.fireTestFinished(desc);
                    }
                });
            }
            else {
                startGrid();

                runTest(script, notifier);
            }
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
        String dirName = test.getParent().toString().substring(scriptsRoot.toString().length() + 1);
        String fileName = test.getFileName().toString();

        if (!fileName.endsWith("test") && fileName.endsWith("test_slow"))
            return;

        Description desc = Description.createTestDescription(dirName, fileName);

        notifier.fireTestStarted(desc);

        try {
            QueryEngine engine = Commons.lookupComponent(
                ((IgniteEx)F.first(Ignition.allGrids())).context(),
                QueryEngine.class
            );

            ScriptTestRunner scriptTestRunner = new ScriptTestRunner(test, engine, rootLog);

            rootLog.info(">>> Start: " + dirName + "/" + fileName);

            scriptTestRunner.run();
        }
        catch (Throwable e) {
            notifier.fireTestFailure(new Failure(desc, e));
        }
        finally {
            rootLog.info(">>> Finish: " + dirName + "/" + fileName);
            notifier.fireTestFinished(desc);
        }
    }

    /** */
    private void startGrid() {
        for (int i = 0; i < nodes; ++i) {
            Ignition.start(
                new IgniteConfiguration()
                    .setIgniteInstanceName("srv" + i)
                    .setDiscoverySpi(
                        new TcpDiscoverySpi()
                            .setIpFinder(sharedFinder)
                    )
                    .setGridLogger(rootLog)
            );
        }
    }
}

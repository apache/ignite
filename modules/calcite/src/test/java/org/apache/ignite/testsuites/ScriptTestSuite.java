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

package org.apache.ignite.testsuites;

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
import org.apache.ignite.internal.processors.query.calcite.logical.ScriptRunnerTestsEnvironment;
import org.apache.ignite.internal.processors.query.calcite.logical.SqlScriptRunner;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.thread.IgniteThread;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Test suite to run SQL test scripts.
 *
 * By default, only "*.test" and "*.test_slow" scripts are run.
 * Other files are ignored.
 *
 * Use {@link ScriptRunnerTestsEnvironment#regex()} property to specify regular expression for filter
 * script path to debug run. In this case the file suffix will be ignored.
 * e.g. regex = "test_aggr_string.test"
 *
 * Use other properties of the {@link ScriptRunnerTestsEnvironment} to setup cluster and test environment.<br><br>
 *
 * All test files consist of appropriate collection of queries.
 * A query record begins with a line of the following form:
 * query <type-string> <sort-mode> <label> <br><br>
 *
 * The SQL for the query is found on second an subsequent lines of the record up to first line of the form "----"
 * or until the end of the record. Lines following the "----" are expected results of the query, one value per line.
 * If the "----" and/or the results are omitted, then the query is expected to return an empty set.
 * The "----" and results are also omitted from prototype scripts and are always ignored when the sqllogictest program
 * is operating in completion mode. Another way of thinking about completion mode is that it copies the script from
 * input to output, replacing all "----" lines and subsequent result values with the actual results from running the
 * query.<br><br>
 *
 * The <type-string> argument to the query statement is a short string that specifies the number of result columns and
 * the expected datatype of each result column. There is one character in the <type-string> for each result column.
 * The characters codes are "T" for a text result, "I" for an integer result, and "R" for a floating-point
 * result.<br><br>
 *
 * The <sort-mode> argument is optional. If included, it must be one of "nosort", "rowsort", or "valuesort".
 * The default is "nosort". In nosort mode, the results appear in exactly the order in which they were received from
 * the database engine. The nosort mode should only be used on queries that have an ORDER BY clause or which only have
 * a single row of result, since otherwise the order of results is undefined and might vary from one database engine
 * to another. The "rowsort" mode gathers all output from the database engine then sorts it by rows on the client side.
 * Sort comparisons use strcmp() on the rendered ASCII text representation of the values. Hence, "9" sorts after "10",
 * not before. The "valuesort" mode works like rowsort except that it does not honor row groupings. Each individual
 * result value is sorted on its own.<br><br>
 *
 * The <label> argument is also optional. If included, sqllogictest stores a hash of the results of this query under
 * the given label. If the label is reused, then sqllogictest verifies that the results are the same.
 * This can be used to verify that two or more queries in the same test script that are logically equivalent
 * always generate the same output.<br><br>
 *
 * In the results section, integer values are rendered as if by printf("%d"). Floating point values are rendered as
 * if by printf("%.3f"). NULL values are rendered as "NULL". Empty strings are rendered as "(empty)". Within non-empty
 * strings, all control characters and unprintable characters are rendered as "@".<br><br>
 *
 * @see <a href="https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki">Extended format documentation.</a></a>
 *
 */
@ScriptRunnerTestsEnvironment(scriptsRoot = "modules/calcite/src/test/sql", timeout = 180000)
public class ScriptTestSuite {
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
    public ScriptTestSuite() {
        ScriptRunnerTestsEnvironment env = ScriptTestSuite.class.getAnnotation(ScriptRunnerTestsEnvironment.class);

        assert !F.isEmpty(env.scriptsRoot());

        nodes = env.nodes();
        scriptsRoot = FS.getPath(U.resolveIgnitePath(env.scriptsRoot()).getPath());
        testRegex = F.isEmpty(env.regex()) ? null : Pattern.compile(env.regex());
        restartCluster = env.restart();
        timeout = env.timeout();
    }

    /**
     * Generates dynamic tests for each script file in the configured directory.
     *
     * @return Stream of dynamic tests.
     * @throws Exception If failed to walk the script directory.
     */
    @TestFactory
    public List<DynamicTest> generateTests() throws Exception {
        // Start cluster if not already started
        if (F.isEmpty(Ignition.allGrids())) {
            startCluster();
        }

        return Files.walk(scriptsRoot)
            .sorted()
            .filter(p -> !p.equals(scriptsRoot))
            .filter(p -> !Files.isDirectory(p))
            .filter(p -> {
                String fileName = p.getFileName().toString();
                return testRegex == null || testRegex.matcher(p.toString()).find();
            })
            .filter(p -> {
                String fileName = p.getFileName().toString();
                if (testRegex == null) {
                    return fileName.endsWith(".test") || fileName.endsWith(".test_slow");
                }
                return true;
            })
            .map(p -> {
                String dirName;
                if (p.getNameCount() - 1 > scriptsRoot.getNameCount())
                    dirName = p.subpath(scriptsRoot.getNameCount(), p.getNameCount() - 1).toString();
                else
                    dirName = scriptsRoot.subpath(scriptsRoot.getNameCount() - 1, scriptsRoot.getNameCount()).toString();

                String fileName = p.getFileName().toString();

                // Restart cluster for each test group (directory) if configured
                Path testDir = p.getParent();
                if (restartCluster && !scriptsRoot.equals(testDir)) {
                    // This is a group (directory) boundary, we'll handle restart in the test execution
                }

                return DynamicTest.dynamicTest(dirName + "/" + fileName, () -> {
                    runSingleTest(p, dirName, fileName);
                });
            })
            .toList();
    }

    /**
     * Runs a single test.
     *
     * @param test Test file path.
     * @param dirName Directory name.
     * @param fileName File name.
     * @throws Exception If test fails.
     */
    private void runSingleTest(Path test, String dirName, String fileName) {
        beforeTest();

        log.info(">>> Start: " + dirName + "/" + fileName);

        try {
            Ignite ign = F.first(Ignition.allGrids());

            QueryEngine engine = Commons.lookupComponent(
                ((IgniteEx)ign).context(),
                QueryEngine.class
            );

            SqlScriptRunner scriptTestRunner = new SqlScriptRunner(test, engine, log);

            try {
                runScript(scriptTestRunner);
            }
            catch (Error | RuntimeException e) {
                throw e;
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        finally {
            log.info(">>> Finish: " + dirName + "/" + fileName);
        }
    }

    /**
     * Cleanup before test.
     */
    void beforeTest() {
        if (F.isEmpty(Ignition.allGrids()))
            startCluster();
        else {
            Ignite ign = F.first(Ignition.allGrids());

            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);
        }
    }

    /**
     * Starts the cluster.
     */
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

    /**
     * Runs the script with timeout support.
     *
     * @param scriptRunner Script runner.
     */
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

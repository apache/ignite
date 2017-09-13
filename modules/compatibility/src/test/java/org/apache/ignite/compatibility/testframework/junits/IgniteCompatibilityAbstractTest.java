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

package org.apache.ignite.compatibility.testframework.junits;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compatibility.testframework.junits.logger.ListenedGridTestLog4jLogger;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.compatibility.testframework.util.MavenUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.Nullable;

/**
 * Super class for all compatibility tests.
 */
public abstract class IgniteCompatibilityAbstractTest extends GridCommonAbstractTest {
    /** Local JVM Ignite node. */
    protected Ignite locJvmInstance = null;

    /** Remote JVM Ignite instance. */
    protected Ignite rmJvmInstance = null;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        TestCompatibilityPluginProvider.enable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        TestCompatibilityPluginProvider.disable();
    }

    /**
     * Gets a path to the default DB working directory.
     *
     * @return Path to the default DB working directory.
     * @throws IgniteCheckedException In case of an error.
     * @see #deleteDefaultDBWorkDirectory()
     * @see #defaultDBWorkDirectoryIsEmpty()
     */
    protected Path getDefaultDbWorkPath() throws IgniteCheckedException {
        return Paths.get(U.defaultWorkDirectory() + File.separator + "db");
    }

    /**
     * Deletes the default DB working directory with all sub-directories and files.
     *
     * @return {@code true} if and only if the file or directory is successfully deleted, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    protected boolean deleteDefaultDBWorkDirectory() throws IgniteCheckedException {
        Path dir = getDefaultDbWorkPath();

        return Files.notExists(dir) || U.delete(dir.toFile());
    }

    /**
     * Checks if the default DB working directory is empty.
     *
     * @return {@code true} if the default DB working directory is empty or doesn't exist, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean defaultDBWorkDirectoryIsEmpty() throws IgniteCheckedException {
        File dir = getDefaultDbWorkPath().toFile();

        return !dir.exists() || (dir.isDirectory() && dir.list().length == 0);
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Starts new grid of given version and index <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param idx Index of the grid to start.
     * @param ver Ignite version.
     * @param cfgClos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(int idx, String ver, IgniteInClosure<IgniteConfiguration> cfgClos) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), ver, cfgClos, null);
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param cfgClos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(String igniteInstanceName, String ver,
        IgniteInClosure<IgniteConfiguration> cfgClos) throws Exception {
        return startGrid(igniteInstanceName, ver, cfgClos, null);
    }

    /**
     * Starts new grid of given version and index <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param idx Index of the grid to start.
     * @param ver Ignite version.
     * @param cfgClos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(int idx, final String ver,
        IgniteInClosure<IgniteConfiguration> cfgClos, IgniteInClosure<Ignite> iClos) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), ver, cfgClos, iClos);
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param cfgClos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(final String igniteInstanceName, final String ver,
        IgniteInClosure<IgniteConfiguration> cfgClos, IgniteInClosure<Ignite> iClos) throws Exception {
        assert isMultiJvm() : "MultiJvm mode must be switched on for the node stop properly.";

        assert !igniteInstanceName.equals(getTestIgniteInstanceName(0)) : "Use default instance name for local nodes only.";

        final String cfgClosPath = CompatibilityTestIgniteNodeRunner.storeToFile(cfgClos);
        final String iClosPath = CompatibilityTestIgniteNodeRunner.storeToFile(iClos);

        final IgniteConfiguration cfg = getConfiguration(igniteInstanceName); // stub - won't be used at node startup

        IgniteProcessProxy ignite = new IgniteProcessProxy(cfg, log, locJvmInstance, true) {
            @Override protected IgniteLogger logger(IgniteLogger log, Object ctgr) {
                return ListenedGridTestLog4jLogger.createLogger(ctgr);
            }

            @Override protected String igniteNodeRunnerClassName() throws Exception {
                return CompatibilityTestIgniteNodeRunner.class.getCanonicalName();
            }

            @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                return cfgClosPath + " " + igniteInstanceName + " " + getId() + (iClosPath == null ? "" : " " + iClosPath);
            }

            @Override protected Collection<String> filteredJvmArgs() throws Exception {
                Collection<String> filteredJvmArgs = new ArrayList<>();

                filteredJvmArgs.add("-ea");

                for (String arg : U.jvmArgs()) {
                    if (arg.startsWith("-Xmx") || arg.startsWith("-Xms"))
                        filteredJvmArgs.add(arg);
                }

                String classPath = System.getProperty("java.class.path");

                String[] paths = classPath.split(File.pathSeparator);

                StringBuilder pathBuilder = new StringBuilder();

                String corePathTemplate = "ignite.modules.core.target.classes".replace(".", File.separator);
                String coreTestsPathTemplate = "ignite.modules.core.target.test-classes".replace(".", File.separator);

                for (String path : paths) {
                    if (!path.contains(corePathTemplate) && !path.contains(coreTestsPathTemplate))
                        pathBuilder.append(path).append(File.pathSeparator);
                }

                String pathToArtifact = MavenUtils.getPathToIgniteCoreArtifact(ver);
                pathBuilder.append(pathToArtifact).append(File.pathSeparator);

                String pathToTestsArtifact = MavenUtils.getPathToIgniteCoreArtifact(ver, "tests");
                pathBuilder.append(pathToTestsArtifact).append(File.pathSeparator);

                filteredJvmArgs.add("-cp");
                filteredJvmArgs.add(pathBuilder.toString());

                return filteredJvmArgs;
            }
        };

        if (rmJvmInstance == null)
            rmJvmInstance = ignite;

        if (locJvmInstance == null) {
            CountDownLatch nodeJoinedLatch = new CountDownLatch(1);

            UUID nodeId = ignite.getId();

            ListenedGridTestLog4jLogger log = (ListenedGridTestLog4jLogger)rmJvmInstance.log();

            log.addListener(nodeId, new LoggedJoinNodeClosure(nodeJoinedLatch, nodeId));

            assert nodeJoinedLatch.await(30, TimeUnit.SECONDS) : "Node has not joined [id=" + nodeId + "]";

            log.removeListener(nodeId);
        }

        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String igniteInstanceName, IgniteConfiguration cfg,
        GridSpringResourceContext ctx) throws Exception {
        Ignite ignite;

        // if started node isn't first node in the local JVM then it was checked earlier for join to topology
        // in IgniteProcessProxy constructor.
        if (locJvmInstance == null && rmJvmInstance != null) {
            CountDownLatch nodeJoinedLatch = new CountDownLatch(1);

            UUID nodeId = cfg.getNodeId();

            ListenedGridTestLog4jLogger log = (ListenedGridTestLog4jLogger)rmJvmInstance.log();

            log.addListener(nodeId, new LoggedJoinNodeClosure(nodeJoinedLatch, nodeId));

            ignite = super.startGrid(igniteInstanceName, cfg, ctx);

            assert ignite.configuration().getNodeId() == cfg.getNodeId() : "Started node have unexpected node id.";

            assert nodeJoinedLatch.await(30, TimeUnit.SECONDS) : "Node has not joined [id=" + nodeId + "]";

            log.removeListener(nodeId);
        }
        else
            ignite = super.startGrid(igniteInstanceName, cfg, ctx);

        if (locJvmInstance == null && !isRemoteJvm(igniteInstanceName))
            locJvmInstance = ignite;

        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected void stopGrid(@Nullable String igniteInstanceName, boolean cancel, boolean awaitTop) {
        if (isRemoteJvm(igniteInstanceName))
            throw new UnsupportedOperationException("Operation isn't supported yet for remotes nodes, use stopAllGrids() instead.");
        else {
            super.stopGrid(igniteInstanceName, cancel, awaitTop);

            locJvmInstance = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void stopAllGrids(boolean cancel) {
        locJvmInstance = null;
        rmJvmInstance = null;

        super.stopAllGrids(cancel);
    }

    /** */
    protected static class LoggedJoinNodeClosure implements IgniteInClosure<String> {
        /** Node joined latch. */
        private CountDownLatch nodeJoinedLatch;

        /** Patterns for comparing. */
        private Set<String> patterns = new HashSet<>();

        /**
         * @param nodeJoinedLatch Node joined latch.
         * @param nodeId Expected node id.
         */
        public LoggedJoinNodeClosure(CountDownLatch nodeJoinedLatch, UUID nodeId) {
            this.nodeJoinedLatch = nodeJoinedLatch;
            this.patterns.add("Remote node has joined [id=" + nodeId + "]");
            this.patterns.add("Remote node has prepared [id=" + nodeId + "]");
        }

        /** {@inheritDoc} */
        @Override public void apply(String s) {
            if (nodeJoinedLatch.getCount() > 0 && patterns.contains(s))
                nodeJoinedLatch.countDown();
        }
    }
}

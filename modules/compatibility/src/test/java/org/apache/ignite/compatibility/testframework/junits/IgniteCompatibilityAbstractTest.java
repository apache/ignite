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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compatibility.testframework.junits.logger.ListenedGridTestLog4jLogger;
import org.apache.ignite.compatibility.testframework.util.CompatibilityTestsUtils;
import org.apache.ignite.compatibility.testframework.util.MavenUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Super class for all compatibility tests.
 */
public abstract class IgniteCompatibilityAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final ClassLoader CLASS_LOADER = IgniteCompatibilityAbstractTest.class.getClassLoader();

    /** Using for synchronization of nodes startup in case of starting remote nodes first. */
    public static final String SYNCHRONIZATION_LOG_MESSAGE = "[Compatibility] Node has been started, id=";

    /** Waiting milliseconds of the join of a node to topology. */
    protected static final int NODE_JOIN_TIMEOUT = 30_000;

    /** Local JVM Ignite node. */
    protected Ignite locJvmInstance = null;

    /** Remote JVM Ignite instance. */
    protected Ignite rmJvmInstance = null;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Starts new Ignite instance of given version and index <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param idx Index of the grid to start.
     * @param ver Ignite version.
     * @param cfgClo IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(int idx, String ver, IgniteInClosure<IgniteConfiguration> cfgClo) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), ver, cfgClo, null);
    }

    /**
     * Starts new Ignite instance of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param cfgClo IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(String igniteInstanceName, String ver,
        IgniteInClosure<IgniteConfiguration> cfgClo) throws Exception {
        return startGrid(igniteInstanceName, ver, cfgClo, null);
    }

    /**
     * Starts new Ignite instance of given version and index <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param idx Index of the grid to start.
     * @param ver Ignite version.
     * @param cfgClo IgniteInClosure for post-configuration.
     * @param clo IgniteInClosure for actions on started Ignite.
     * @return Started grid.
     * @throws Exception In case of an error.
     */
    protected IgniteEx startGrid(int idx, final String ver,
        IgniteInClosure<IgniteConfiguration> cfgClo, IgniteInClosure<Ignite> clo) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), ver, cfgClo, clo);
    }

    /**
     * Starts new Ignite instance of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version. Dots separated, 3-digit version.
     * @param cfgClo IgniteInClosure for post-configuration.
     * @param clo IgniteInClosure for actions on started Ignite.
     * @return Started grid.
     * @throws Exception In case of an error.
     */
    protected IgniteEx startGrid(final String igniteInstanceName, final String ver,
        IgniteInClosure<IgniteConfiguration> cfgClo, IgniteInClosure<Ignite> clo) throws Exception {
        assert isMultiJvm() : "MultiJvm mode must be switched on for the node stop properly.";

        assert !igniteInstanceName.equals(getTestIgniteInstanceName(0)) : "Use default instance name for local nodes only.";

        final String cfgCloPath = IgniteCompatibilityNodeRunner.storeToFile(cfgClo);
        final String cloPath = IgniteCompatibilityNodeRunner.storeToFile(clo);

        final IgniteConfiguration cfg = getConfiguration(igniteInstanceName); // stub - won't be used at node startup

        IgniteProcessProxy ignite = new IgniteProcessProxy(cfg, log, locJvmInstance == null ? null : (x) -> locJvmInstance, true) {
            @Override protected IgniteLogger logger(IgniteLogger log, Object ctgr) {
                return ListenedGridTestLog4jLogger.createLogger(ctgr + "#" + ver.replaceAll("\\.", "_"));
            }

            @Override protected String igniteNodeRunnerClassName() throws Exception {
                return IgniteCompatibilityNodeRunner.class.getCanonicalName();
            }

            @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                return cfgCloPath + " " + igniteInstanceName + " "
                    + getId() + " "
                    + (rmJvmInstance == null ? getId() : ((IgniteProcessProxy)rmJvmInstance).getId()) + " "
                    + ver
                    + (cloPath == null ? "" : " " + cloPath);
            }

            @Override protected Collection<String> filteredJvmArgs() throws Exception {
                return getProcessProxyJvmArgs(ver);
            }
        };

        if (locJvmInstance == null) {
            CountDownLatch nodeJoinedLatch = new CountDownLatch(1);

            UUID nodeId = ignite.getId();

            ListenedGridTestLog4jLogger log = (ListenedGridTestLog4jLogger)ignite.log();

            log.addListener(nodeId, new LoggedJoinNodeClosure(nodeJoinedLatch, nodeId));

            final long nodeJoinTimeout = getNodeJoinTimeout();
            final boolean joined = nodeJoinedLatch.await(nodeJoinTimeout, TimeUnit.MILLISECONDS);

            assertTrue("Node has not joined [id=" + nodeId + "]/" +
                "or does not completed its startup during timeout: " + nodeJoinTimeout + " ms.", joined);

            log.removeListener(nodeId);
        }

        if (rmJvmInstance == null)
            rmJvmInstance = ignite;

        return ignite;
    }

    /**
     * Creates list of JVM arguments to be used to start new Ignite process in separate JVM.
     */
    protected Collection<String> getProcessProxyJvmArgs(String ver) throws Exception {
        Collection<String> filteredJvmArgs = new ArrayList<>();

        filteredJvmArgs.add("-ea");

        for (String arg : U.jvmArgs()) {
            if (arg.startsWith("-Xmx") || arg.startsWith("-Xms"))
                filteredJvmArgs.add(arg);
        }

        final Collection<Dependency> dependencies = getDependencies(ver);

        Set<String> excluded = getExcluded(ver, dependencies);

        StringBuilder pathBuilder = new StringBuilder();

        for (URL url : CompatibilityTestsUtils.classLoaderUrls(CLASS_LOADER)) {
            String path = url.getPath();

            if (excluded.stream().noneMatch(path::contains))
                pathBuilder.append(path).append(File.pathSeparator);
        }

        for (Dependency dependency : dependencies) {
            final String artifactVer = Optional.ofNullable(dependency.version()).orElse(ver);

            String pathToArtifact = MavenUtils.getPathToIgniteArtifact(dependency.groupId(),
                    dependency.artifactId(), artifactVer, dependency.classifier());

            pathBuilder.append(pathToArtifact).append(File.pathSeparator);
        }

        filteredJvmArgs.add("-cp");
        filteredJvmArgs.add(pathBuilder.toString());

        final Collection<String> jvmParms = getJvmParams();

        if (jvmParms != null)
            filteredJvmArgs.addAll(jvmParms);

        return filteredJvmArgs;
    }

    /**
     * Total amount of milliseconds.
     *
     * @return timeout in ms.
     */
    protected long getNodeJoinTimeout() {
        return NODE_JOIN_TIMEOUT;
    }

    /**
     * @return list of actual module dependencies from pom.xml
     */
    @NotNull protected Collection<Dependency> getDependencies(String igniteVer) {
        final Collection<Dependency> dependencies = new ArrayList<>();

        dependencies.add(new Dependency("core", "ignite-core", false));
        dependencies.add(new Dependency("core", "ignite-core", true));

        return dependencies;
    }

    /**
     * These dependencies will not be translated from current code dependencies into separate node's classpath.
     *
     * Include here all dependencies which will be set up manually, leave all version independent dependencies.
     *
     * @param ver Ignite version.
     * @param dependencies Dependencies to filter.
     * @return Set of paths to exclude.
     */
    protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = new HashSet<>();

        for (Dependency dependency : dependencies) {
            excluded.add(dependency.sourcePathTemplate());
            excluded.add(dependency.artifactPathTemplate());
        }

        // Just to exclude indexing module
        excluded.add("indexing");

        return excluded;
    }

    /**
     * Allows to setup JVM arguments for standalone JVM
     *
     * @return additional JVM arguments
     */
    protected Collection<String> getJvmParams() {
        return new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String igniteInstanceName, IgniteConfiguration cfg,
        GridSpringResourceContext ctx) throws Exception {
        final Ignite ignite;

        // if started node isn't first node in the local JVM then it was checked earlier for join to topology
        // in IgniteProcessProxy constructor.
        if (locJvmInstance == null && rmJvmInstance != null) {
            final UUID nodeId = cfg.getNodeId();
            final UUID syncNodeId = ((IgniteProcessProxy)rmJvmInstance).getId();

            ignite = super.startGrid(igniteInstanceName, cfg, ctx);

            assert ignite.configuration().getNodeId() == nodeId : "Started node has unexpected node id.";

            assert ignite.cluster().node(syncNodeId) != null : "Node has not joined [id=" + nodeId + "]";
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

        /** Pattern for comparing. */
        private String pattern;

        /**
         * @param nodeJoinedLatch Nodes startup synchronization latch.
         * @param nodeId Expected node id.
         */
        LoggedJoinNodeClosure(CountDownLatch nodeJoinedLatch, UUID nodeId) {
            this.nodeJoinedLatch = nodeJoinedLatch;
            this.pattern = SYNCHRONIZATION_LOG_MESSAGE + nodeId;
        }

        /** {@inheritDoc} */
        @Override public void apply(String s) {
            if (nodeJoinedLatch.getCount() > 0 && s.contains(pattern))
                nodeJoinedLatch.countDown();
        }
    }
}

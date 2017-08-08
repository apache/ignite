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
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.compatibility.testframework.util.MavenUtils;

/**
 * Super class for all compatibility tests.
 */
public abstract class IgniteCompatibilityAbstractTest extends GridCommonAbstractTest {
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

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param clos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String igniteInstanceName, final String ver,
        IgniteInClosure<IgniteConfiguration> clos) throws Exception {
        assert isMultiJvm() : "MultiJvm mode must be switched on for the node stop properly.";

        assert !isFirstGrid(igniteInstanceName) : "Please, start node of current version first.";

        final String closPath = CompatibilityTestIgniteNodeRunner.storeToFile(clos);

        return new IgniteProcessProxy(getConfiguration(), log, grid(0), true) {
            @Override protected String getNodeRunnerClassName() throws Exception {
                return CompatibilityTestIgniteNodeRunner.class.getCanonicalName();
            }

            @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                return closPath + " " + getId();
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
    }
}

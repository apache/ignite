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

package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 *
 */
public abstract class GridUriDeploymentAbstractSelfTest extends GridSpiAbstractTest<UriDeploymentSpi> {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        getSpi().setListener(null);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        getSpi().setListener(new DeploymentListener() {
            @Override public void onUnregistered(ClassLoader ldr) {
                // No-op.
            }
        });
    }

    /**
     * @return Temporary directory to be used in test.
     */
    @GridSpiTestConfig
    public String getTemporaryDirectoryPath() {
        String path = GridTestProperties.getProperty("deploy.uri.tmpdir");

        assert path != null;

        return path;
    }

    /**
     * @param taskName Name of available task.
     * @throws Exception if failed.
     */
    protected void checkTask(String taskName) throws Exception {
        assert taskName != null;

        DeploymentResource task = getSpi().findResource(taskName);

        assert task != null;

        info("Deployed task [task=" + task + ']');
    }

    /**
     * @param taskName name of unavailable task.
     * @throws Exception if failed.
     */
    protected void checkNoTask(String taskName) throws Exception {
        assert taskName != null;

        DeploymentResource task = getSpi().findResource(taskName);

        assert task == null;

        info("Not deployed task [task=" + task + ']');
    }

    /**
     * @param taskName name of task to wait on.
     * @param expectDeployed if {@code true}, wait for availability, else wait for unavailability.
     * @param timeout in ms.
     * @throws Exception if failed.
     */
    protected void waitForTask(String taskName, boolean expectDeployed, long timeout) throws IgniteCheckedException {
        assertTrue("Failed to wait for (un)deployment of " + taskName,
            GridTestUtils.waitForCondition(new GridAbsPredicateX() {
                public boolean applyx() throws IgniteCheckedException {
                    if (expectDeployed)
                        return getSpi().findResource(taskName) != null;
                    else
                        return getSpi().findResource(taskName) == null;
                }
            }, timeout));

        info((expectDeployed ? "Deployed" : "Not deployed") + " task [task=" + taskName + ']');
    }
}

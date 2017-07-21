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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Test for classloader registering.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentClassloaderRegisterSelfTest extends GridSpiAbstractTest<UriDeploymentSpi> {
    /** */
    private static Map<ClassLoader, Set<Class<? extends ComputeTask<?, ?>>>> tasks =
        Collections.synchronizedMap(new HashMap<ClassLoader, Set<Class<? extends ComputeTask<?, ?>>>>());

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        getSpi().setListener(null);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        getSpi().setListener(new DeploymentListener() {
            @Override public void onUnregistered(ClassLoader ldr) { tasks.remove(ldr); }
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        tasks.clear();
    }

    /**
     * @param taskCls Class to be deployed.
     * @throws Exception if deployment failed.
     */
    private void deploy(Class<? extends ComputeTask<?, ?>> taskCls) throws Exception {
        getSpi().register(taskCls.getClassLoader(), taskCls);

        Set<Class<? extends ComputeTask<?, ?>>> clss = new HashSet<>(1);

        clss.add(taskCls);

        tasks.put(taskCls.getClassLoader(), clss);
    }

    /**
     * @param taskCls Unavailable task class.
     */
    private void checkUndeployed(Class<? extends ComputeTask<?, ?>> taskCls) {
        assert !tasks.containsKey(taskCls.getClassLoader());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploy() throws Exception {
        Class<? extends ComputeTask<?, ?>> task = GridFileDeploymentTestTask.class;

        deploy(task);

        DeploymentResource t1 = getSpi().findResource(task.getName());

        assert t1 != null;

        DeploymentResource t2 = getSpi().findResource(task.getName());

        assert t1.equals(t2);
        assert t1.getResourceClass() == t2.getResourceClass();

        getSpi().unregister(task.getName());

        checkUndeployed(task);

        assert getSpi().findResource(task.getName()) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeploy() throws Exception {
        // Test non-versioned redeploy.
        Class<? extends ComputeTask<?, ?>> t1 = GridFileDeploymentTestTask.class;

        deploy(t1);

        Class<? extends ComputeTask<?, ?>> t2 = GridFileDeploymentTestTask.class;

        deploy(t2);

        getSpi().unregister(t1.getName());

        checkUndeployed(t1);
        checkUndeployed(t2);
    }

    /**
     * @return List of URIs to use in this test.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        // No real gar file is required. Add one just to avoid failure because of missed to default directory.
        return Collections.singletonList(GridTestProperties.getProperty("ant.urideployment.gar.uri").
            replace("EXTDATA", U.resolveIgnitePath("modules/extdata").getAbsolutePath()));
    }

    /**
     * Do nothing task for test.
     */
    private static class GridFileDeploymentTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
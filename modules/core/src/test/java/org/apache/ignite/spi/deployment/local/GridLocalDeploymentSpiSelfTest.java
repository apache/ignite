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

package org.apache.ignite.spi.deployment.local;

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
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Local deployment SPI test.
 */
@GridSpiTest(spi = LocalDeploymentSpi.class, group = "Deployment SPI")
public class GridLocalDeploymentSpiSelfTest extends GridSpiAbstractTest<LocalDeploymentSpi> {
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
     * @param taskCls Task class.
     * @throws Exception If failed.
     */
    private void deploy(Class<? extends ComputeTask<?, ?>> taskCls) throws Exception {
        getSpi().register(taskCls.getClassLoader(), taskCls);

        Set<Class<? extends ComputeTask<?, ?>>> clss = new HashSet<>(1);

        clss.add(taskCls);

        tasks.put(LocalDeploymentSpi.class.getClassLoader(), clss);
    }

    /**
     * @param taskCls Task class.
     */
    private void checkUndeployed(Class<? extends ComputeTask<?, ?>> taskCls) {
        assert !tasks.containsKey(taskCls.getClassLoader());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testDeploy() throws Exception {
        String taskName = "GridDeploymentTestTask";

        Class<? extends ComputeTask<?, ?>> task = GridDeploymentTestTask.class;

        deploy(task);

        // Note we use task name instead of class name.
        DeploymentResource t1 = getSpi().findResource(taskName);

        assert t1 != null;

        assert t1.getResourceClass().equals(task);
        assert t1.getName().equals(taskName);

        getSpi().unregister(taskName);

        checkUndeployed(task);

        assert getSpi().findResource(taskName) == null;
        assert getSpi().findResource(task.getName()) == null;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testRedeploy() throws Exception {
        String taskName = "GridDeploymentTestTask";

        // Test versioned redeploy.
        Class<? extends ComputeTask<?, ?>> t1 = GridDeploymentTestTask.class;
        Class<? extends ComputeTask<?, ?>> t2 = GridDeploymentTestTask1.class;

        deploy(t1);

        try {
            deploy(t2);

            assert false : "Exception must be thrown for registering with the same name.";
        }
        catch (IgniteSpiException ignored) {
            // No-op.
        }

        getSpi().unregister("GridDeploymentTestTask");

        checkUndeployed(t1);

        assert getSpi().findResource("GridDeploymentTestTask") == null;

        tasks.clear();

        deploy(t1);

        try {
            deploy(t2);

            assert false : "Exception must be thrown for registering with the same name.";
        }
        catch (IgniteSpiException ignored) {
            // No-op.
        }

        getSpi().unregister(t1.getName());

        checkUndeployed(t1);

        assert getSpi().findResource(taskName) == null;
        assert getSpi().findResource(t1.getName()) == null;
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass", "InnerClassMayBeStatic"})
    @ComputeTaskName(value="GridDeploymentTestTask")
    public class GridDeploymentTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass", "InnerClassMayBeStatic"})
    @ComputeTaskName(value="GridDeploymentTestTask")
    public class GridDeploymentTestTask1 extends ComputeTaskSplitAdapter<Object, Object> {
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

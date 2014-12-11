/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.local;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.deployment.*;
import org.gridgain.testframework.junits.spi.*;

import java.io.*;
import java.util.*;

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
        catch (IgniteSpiException e) {
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
        catch (IgniteSpiException e) {
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}

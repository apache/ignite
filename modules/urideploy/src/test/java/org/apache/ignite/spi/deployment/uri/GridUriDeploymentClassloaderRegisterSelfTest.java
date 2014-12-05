/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

import java.io.*;
import java.util.*;

/**
 * Test for classloader registering.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentClassloaderRegisterSelfTest extends GridSpiAbstractTest<GridUriDeploymentSpi> {
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
            replace("EXTDATA", U.resolveGridGainPath("modules/extdata").getAbsolutePath()));
    }

    /**
     * Do nothing task for test.
     */
    private static class GridFileDeploymentTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            return null;
        }
        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) throws GridException {
            return null;
        }
    }
}

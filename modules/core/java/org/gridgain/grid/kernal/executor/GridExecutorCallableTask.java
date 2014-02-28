// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.executor;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class defines own implementation for {@link GridComputeTask}. This class used by
 * {@link GridExecutorService} when commands submitted and can be
 * randomly assigned to available grid nodes. This grid task creates only one
 * {@link GridComputeJob} and transfer it to any available node. See {@link GridComputeTaskSplitAdapter}
 * for more details.
 *
 * @author @java.author
 * @version @java.version
 * @param <T> Return type of {@link Callable}.
 */
@SuppressWarnings({"TransientFieldNotInitialized"})
public class GridExecutorCallableTask<T> extends GridComputeTaskAdapter<Callable<T>, T> {
    /** Deploy class. */
    private final transient Class<?> p2pCls;

    /** Load balancer. */
    @GridLoadBalancerResource
    private GridComputeLoadBalancer balancer;

    /**
     * Creates callable task with given deployment class.
     *
     * @param cls Deployment class for peer-deployment.
     */
    public GridExecutorCallableTask(Class<?> cls) {
        assert cls != null;

        p2pCls = cls;
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        return p2pCls;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return p2pCls.getClassLoader();
    }

    /**
     * Create job.
     *
     * @param arg Callable argument.
     * @return Grid job.
     */
    private GridComputeJob createJob(Callable<T> arg) {
        return new GridComputeJobAdapter(arg) {
            /** */
            @GridInstanceResource
            private Grid grid;

            /*
            * Simply execute command passed into the job and
            * returns result.
            */
            @SuppressWarnings("unchecked")
            @Override public Object execute() throws GridException {
                Callable<T> call = (Callable<T>)argument(0);

                if (call != null) {
                    GridKernalContext ctx = ((GridKernal)grid).context();

                    ctx.resource().inject(ctx.deploy().getDeployment(call.getClass().getName()), call.getClass(), call);

                    // Execute command.
                    try {
                        return call.call();
                    }
                    catch (Exception e) {
                        throw new GridException("Failed to execute command.", e);
                    }
                }

                return null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public final Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Callable<T> arg) throws GridException {
        assert subgrid != null;
        assert !subgrid.isEmpty();

        GridComputeJob job = createJob(arg);

        return Collections.singletonMap(job, balancer.getBalancedNode(job, null));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public T reduce(List<GridComputeJobResult> results) throws GridException {
        assert results != null;
        assert results.size() == 1;

        return (T)results.get(0).getData();
    }
}

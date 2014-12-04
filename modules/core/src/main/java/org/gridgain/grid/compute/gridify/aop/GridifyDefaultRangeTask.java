/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute.gridify.aop;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.gridify.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.util.gridify.GridifyUtils.*;

/**
 * Default gridify task which simply executes a method on remote node.
 * <p/>
 * See {@link Gridify} documentation for more information about execution of {@code gridified} methods.
 * @see GridifySetToSet
 * @see GridifySetToValue
 */
public class GridifyDefaultRangeTask extends GridComputeTaskAdapter<GridifyRangeArgument, Collection<?>>
    implements GridPeerDeployAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deploy class. */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private final transient Class<?> p2pCls;

    /** Grid instance. */
    @GridInstanceResource
    private Ignite ignite;

    /** Grid task session. */
    @GridTaskSessionResource
    private GridComputeTaskSession ses;

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Load balancer. */
    @GridLoadBalancerResource
    private GridComputeLoadBalancer balancer;

    /** */
    @SuppressWarnings({"UnusedDeclaration"})
    @GridTaskContinuousMapperResource
    private GridComputeTaskContinuousMapper mapper;

    /** */
    private GridifyNodeFilter nodeFilter;

    /** */
    private int splitSize;

    /** */
    private int threshold;

    /** */
    private boolean limitedSplit;

    /**
     * @param cls Deployment class.
     * @param nodeFilter Predicate node filter.
     * @param threshold Parameter that defines the minimal value below which the
     *      execution will NOT be grid-enabled.
     * @param splitSize Split size for job arguments.
     * @param limitedSplit Indicates limitation for split algorithm.
     */
    public GridifyDefaultRangeTask(Class<?> cls, GridifyNodeFilter nodeFilter, int threshold, int splitSize,
        boolean limitedSplit) {
        assert cls != null;

        p2pCls = cls;

        this.nodeFilter = nodeFilter;
        this.threshold = threshold;
        this.splitSize = splitSize;
        this.limitedSplit = limitedSplit;
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        return p2pCls;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return U.detectClassLoader(p2pCls);
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, GridifyRangeArgument arg)
        throws GridException {
        assert !subgrid.isEmpty() : "Subgrid should not be empty: " + subgrid;

        assert ignite != null : "Grid instance could not be injected";

        if (splitSize < threshold && splitSize != 0 && threshold != 0) {
            throw new GridException("Incorrect Gridify annotation parameters. Value for parameter " +
                "'splitSize' should not be less than parameter 'threshold' [splitSize=" + splitSize +
                ", threshold=" + threshold + ']');
        }

        Collection<GridNode> exclNodes = new LinkedList<>();

        // Filter nodes.
        if (nodeFilter != null) {
            for (GridNode node : subgrid) {
                if (!nodeFilter.apply(node, ses))
                    exclNodes.add(node);
            }

            if (exclNodes.size() == subgrid.size())
                throw new GridException("Failed to execute on grid where all nodes excluded.");
        }

        int inputPerNode = splitSize;

        // Calculate input elements size per node for default annotation splitSize parameter.
        if (splitSize <= 0) {
            // For iterable input splitSize will be assigned with threshold value.
            if (threshold > 0 && arg.getInputSize() == UNKNOWN_SIZE)
                inputPerNode = threshold;
            // Otherwise, splitSize equals (inputSize / nodesCount)
            else {
                assert arg.getInputSize() != UNKNOWN_SIZE;

                int gridSize = subgrid.size() - exclNodes.size();

                gridSize = (gridSize <= 0 ? subgrid.size() : gridSize);

                inputPerNode = calculateInputSizePerNode(gridSize, arg.getInputSize(), threshold, limitedSplit);

                if (log.isDebugEnabled()) {
                    log.debug("Calculated input elements size per node [inputSize=" + arg.getInputSize() +
                        ", gridSize=" + gridSize + ", threshold=" + threshold +
                        ", limitedSplit=" + limitedSplit + ", inputPerNode=" + inputPerNode + ']');
                }
            }
        }

        GridifyArgumentBuilder argBuilder = new GridifyArgumentBuilder();

        Iterator<?> inputIter = arg.getInputIterator();

        while (inputIter.hasNext()) {
            Collection<Object> nodeInput = new LinkedList<>();

            for (int i = 0; i < inputPerNode && inputIter.hasNext(); i++)
                nodeInput.add(inputIter.next());

            // Create job argument.
            GridifyArgument jobArg = argBuilder.createJobArgument(arg, nodeInput);

            GridComputeJob job = new GridifyJobAdapter(jobArg);

            mapper.send(job, balancer.getBalancedNode(job, exclNodes));
        }

        // Map method can return null because job already sent by continuous mapper.
        return null;
    }

    /** {@inheritDoc} */
    @Override public final Collection<?> reduce(List<GridComputeJobResult> results) throws GridException {
        assert results.size() >= 1;

        Collection<Object> data = new ArrayList<>(results.size());

        for (GridComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            data.add(res.getData());
        }

        return data;
    }

    /**
     * Calculate count of elements from input to send in job as argument.
     *
     * @param gridSize Grid size.
     * @param inputSize Input collection size.
     * @param threshold Restricts the number of elements from input that used as job execution argument.
     * @param limitedSplit Restricts split for {@link GridifySetToValue} annotation.
     * @return Maximum count of elements from input to send in job as argument.
     */
    private int calculateInputSizePerNode(int gridSize, int inputSize, int threshold, boolean limitedSplit) {
        if (threshold > 0) {
            assert inputSize > threshold;

            int inputPerNode = (int)Math.ceil((double)inputSize / (double)gridSize);

            while (inputSize % inputPerNode <= threshold)
                inputPerNode++;

            return inputPerNode;
        }

        // Use only one node for calculation.
        if (limitedSplit && inputSize <= gridSize)
            return inputSize;

        int inputPerNode = (int)Math.ceil((double)inputSize / (double)gridSize);

        while (inputSize % inputPerNode == 1)
            inputPerNode++;

        return inputPerNode;
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute.gridify.aop;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.gridify.*;
import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.util.gridify.GridifyUtils.*;

/**
 * Convenience adapter with common methods for different aspect implementations
 * (AspectJ, JBoss AOP, Spring AOP).
 * This adapter used in grid task for {@link GridifySetToValue} annotation.
 */
public class GridifySetToValueAbstractAspect {
    /**
     * Check method signature.
     *
     * @param mtd Grid-enabled method.
     * @throws GridException If method signature invalid.
     */
    protected void checkMethodSignature(Method mtd) throws GridException {
        Class<?>[] paramTypes = mtd.getParameterTypes();

        Collection<Integer> allowedParamIdxs = new LinkedList<>();

        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];

            if (GridifyUtils.isMethodParameterTypeAllowed(paramType))
                allowedParamIdxs.add(i);
        }

        if (allowedParamIdxs.isEmpty()) {
            throw new GridException("Invalid method signature. Failed to get valid method parameter types " +
                "[mtdName=" + mtd.getName() + ", allowedTypes=" + GridifyUtils.getAllowedMethodParameterTypes() + ']');
        }

        List<Integer> annParamIdxs = new LinkedList<>();

        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];

            if (GridifyUtils.isMethodParameterTypeAnnotated(paramType.getDeclaredAnnotations()))
                annParamIdxs.add(i);
        }

        if (annParamIdxs.size() > 1) {
            throw new GridException("Invalid method signature. Only one method parameter can may annotated with @" +
                GridifyInput.class.getSimpleName() +
                "[mtdName=" + mtd.getName() + ", allowedTypes=" + GridifyUtils.getAllowedMethodParameterTypes() +
                ", annParamIdxs=" + annParamIdxs + ']');
        }

        if (allowedParamIdxs.size() > 1 && annParamIdxs.isEmpty()) {
            throw new GridException("Invalid method signature. Method parameter must be annotated with @" +
                GridifyInput.class.getSimpleName() +
                "[mtdName=" + mtd.getName() + ", allowedTypes=" + GridifyUtils.getAllowedMethodParameterTypes() +
                ", allowedParamIdxs=" + allowedParamIdxs + ']');
        }

        if (!annParamIdxs.isEmpty() && !allowedParamIdxs.contains(annParamIdxs.get(0))) {
            throw new GridException("Invalid method signature. Invalid annotated parameter " +
                "[mtdName=" + mtd.getName() + ", allowedTypes=" + GridifyUtils.getAllowedMethodParameterTypes() +
                ", allowedParamIdxs=" + allowedParamIdxs + ", annParamIdxs=" + annParamIdxs + ']');
        }
    }

    /**
     * Check if split allowed for input argument.
     * Note, that for argument with unknown elements size with
     * default {@link GridifySetToValue#splitSize()} value (default value {@code 0})
     * there is no ability to calculate how much jobs should be used in task execution.
     *
     * @param arg Gridify argument.
     * @param ann Annotation
     * @throws GridException If split is not allowed with current parameters.
     */
    protected void checkIsSplitToJobsAllowed(GridifyRangeArgument arg, GridifySetToValue ann) throws GridException {
        if (arg.getInputSize() == UNKNOWN_SIZE && ann.threshold() <= 0 && ann.splitSize() <= 0) {
            throw new GridException("Failed to split input method argument to jobs with unknown input size and " +
                "invalid annotation parameter 'splitSize' [mtdName=" + arg.getMethodName() + ", inputTypeCls=" +
                arg.getMethodParameterTypes()[arg.getParamIndex()].getName() +
                ", threshold=" + ann.threshold() + ", splitSize=" + ann.splitSize() + ']');
        }
    }

    /**
     * Execute method on grid.
     *
     * @param mtd Method.
     * @param compute {@link org.apache.ignite.IgniteCompute} instance.
     * @param cls Joint point signature class.
     * @param arg GridifyArgument with all method signature parameters.
     * @param nodeFilter Node filter.
     * @param threshold Parameter that defines the minimal value below which the
     *      execution will NOT be grid-enabled.
     * @param splitSize Size of elements to send in job argument.
     * @param timeout Execution timeout.
     * @return Result.
     * @throws GridException If execution failed.
     */
    protected Object execute(Method mtd, IgniteCompute compute, Class<?> cls, GridifyRangeArgument arg,
        GridifyNodeFilter nodeFilter, int threshold, int splitSize, long timeout) throws GridException {
        long now = U.currentTimeMillis();

        long end = timeout == 0 ? Long.MAX_VALUE : timeout + now;

        // Prevent overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        Collection<?> res = null;

        while (true) {
            if (now > end)
                throw new ComputeTaskTimeoutException("Timeout occurred while waiting for completion.");

            GridifyRangeArgument taskArg = createGridifyArgument(arg, res);

            if (taskArg == null)
                return result(res);
            else if (taskArg.getInputSize() != UNKNOWN_SIZE && taskArg.getInputSize() <= threshold) {
                // Note, that we can't cancel by timeout locally started method.
                try {
                    mtd.setAccessible(true);

                    return mtd.invoke(arg.getTarget(), taskArg.getMethodParameters());
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new GridException("Failed to execute method locally.", e);
                }
            }
            else {
                res = compute.withTimeout(timeout == 0 ? 0L : (end - now)).execute(
                    new GridifyDefaultRangeTask(cls, nodeFilter, threshold, splitSize, true), taskArg);
            }

            now = U.currentTimeMillis();
        }
    }

    /**
     * Prepare task argument for next task execution.
     *
     * @param arg GridifyArgument with all method signature parameters.
     * @param taskRes Result of last task execution.
     * @return New gridify argument or {@code null} if calculation finished.
     * @throws GridException In case of error.
     */
    private GridifyRangeArgument createGridifyArgument(GridifyRangeArgument arg, Collection<?> taskRes)
        throws GridException {
        // If first run.
        if (taskRes == null)
            return arg;

        if (taskRes.size() == 1)
            return null;

        return new GridifyArgumentBuilder().createTaskArgument(arg, taskRes);
    }

    /**
     * Handle last calculation result.
     *
     * @param res Result of last task execution.
     * @return Calculation result.
     */
    private Object result(Collection<?> res) {
        assert res != null;

        assert res.size() == 1;

        return res.iterator().next();
    }
}

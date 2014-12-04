/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute.gridify.aop;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.compute.gridify.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.gridify.*;
import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.util.gridify.GridifyUtils.UNKNOWN_SIZE;

/**
 * Convenience adapter with common methods for different aspect implementations
 * (AspectJ, JBoss AOP, Spring AOP).
 * This adapter used in grid task for {@link GridifySetToSet} annotation.
 */
public class GridifySetToSetAbstractAspect {
    /**
     * Check method signature.
     *
     * @param mtd Grid-enabled method.
     * @throws GridException If method signature invalid..
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

        if (!GridifyUtils.isMethodReturnTypeValid(mtd.getReturnType())) {
            throw new GridException("Invalid method signature. Invalid method return type " +
                "[mtdName=" + mtd.getName() + ", allowedTypes=" + GridifyUtils.getAllowedMethodReturnTypes() +
                ", mtdReturnType=" + mtd.getReturnType() + ']');
        }
    }

    /**
     * Check if split allowed for input argument.
     * Note, that for argument with unknown elements size with
     * default {@link GridifySetToSet#splitSize()} value (default value {@code 0})
     * there is no ability to calculate how much jobs should be used in task execution.
     *
     * @param arg Gridify argument.
     * @param ann Annotation
     * @throws GridException If split is not allowed with current parameters.
     */
    protected void checkIsSplitToJobsAllowed(GridifyRangeArgument arg, GridifySetToSet ann) throws GridException {
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
    protected Object execute(IgniteCompute compute, Class<?> cls, GridifyRangeArgument arg,
        GridifyNodeFilter nodeFilter, int threshold, int splitSize, long timeout) throws GridException {
        long now = U.currentTimeMillis();

        long end = timeout == 0 ? Long.MAX_VALUE : timeout + now;

        // Prevent overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        if (now > end)
            throw new ComputeTaskTimeoutException("Timeout occurred while waiting for completion.");

        Collection<?> res = compute.withTimeout(timeout == 0 ? 0L : (end - now)).execute(
            new GridifyDefaultRangeTask(cls, nodeFilter, threshold, splitSize, false),
            arg);

        return result(arg.getMethodReturnType(), res);
    }

    /**
     * Handle last calculation result.
     *
     * @param cls Method return type.
     * @param taskRes Result of last task execution.
     * @return Calculation result.
     */
    @SuppressWarnings({"unchecked"})
    private Object result(Class<?> cls, Iterable taskRes) {
        assert taskRes != null;

        Collection<Object> res = new LinkedList<>();

        for (Object element : taskRes) {
            res.addAll(GridifyUtils.parameterToCollection(element));
        }

        return GridifyUtils.collectionToParameter(cls, res);
    }
}

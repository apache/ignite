/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.gridify;

import org.apache.ignite.compute.gridify.*;
import org.apache.ignite.compute.gridify.aop.*;
import org.gridgain.grid.*;

import java.lang.annotation.*;
import java.util.*;

/**
 * Argument builder used for creating arguments for tasks and jobs in gridified methods.
 * @see GridifySetToValue
 * @see GridifySetToSet
 */
public final class GridifyArgumentBuilder {
    /**
     * Create argument for task.
     *
     * @param mtdCls Method class.
     * @param mtdName Method name.
     * @param mtdReturnType Method return type.
     * @param mtdTypes Method parameter types.
     * @param mtdParamAnns Method parameter annotations.
     * @param mtdParams Method parameters.
     * @param mtdTarget Target object.
     * @return Argument for task.
     */
    public GridifyRangeArgument createTaskArgument(
        Class<?> mtdCls,
        String mtdName,
        Class<?> mtdReturnType,
        Class<?>[] mtdTypes,
        Annotation[][] mtdParamAnns,
        Object[] mtdParams,
        Object mtdTarget) {

        GridifyRangeArgument arg = new GridifyRangeArgument();

        arg.setMethodClass(mtdCls);
        arg.setMethodName(mtdName);
        arg.setMethodReturnType(mtdReturnType);
        arg.setMethodParameterTypes(mtdTypes);
        arg.setMethodParameters(mtdParams);
        arg.setTarget(mtdTarget);

        arg.setParamIndex(findMethodParameterIndex(mtdName, mtdTypes, mtdParamAnns));

        return arg;
    }

    /**
     * Find parameter index where elements should be placed.
     *
     * @param mtdName Method name.
     * @param mtdTypes Method parameter types.
     * @param mtdParamAnns Method parameter annotations.
     * @return Parameter index where elements should be placed.
     */
    private int findMethodParameterIndex(
        String mtdName,
        Class<?>[] mtdTypes,
        Annotation[][] mtdParamAnns) {
        List<Integer> allowedParamIdxs = new ArrayList<>(mtdTypes.length);

        for (int i = 0; i < mtdTypes.length; i++) {
            Class<?> paramType = mtdTypes[i];

            if (GridifyUtils.isMethodParameterTypeAllowed(paramType))
                allowedParamIdxs.add(i);
        }

        assert !allowedParamIdxs.isEmpty() : "Invalid method signature. Failed to get valid method parameter " +
            "types [mtdName=" + mtdName + ", mtdTypes=" + Arrays.asList(mtdTypes) + ']';

        if (allowedParamIdxs.size() == 1)
            return allowedParamIdxs.get(0);

        List<Integer> annParamIdxs = new ArrayList<>(mtdTypes.length);

        for (int i = 0; i < mtdTypes.length; i++) {
            if (GridifyUtils.isMethodParameterTypeAnnotated(mtdParamAnns[i]))
                annParamIdxs.add(i);
        }

        assert annParamIdxs.size() == 1 : "Invalid method signature. Method parameter must be annotated with @" +
                GridifyInput.class.getSimpleName() + "[mtdName=" + mtdName + ", mtdTypes=" + Arrays.asList(mtdTypes) +
                ", allowedParamIdxs=" + allowedParamIdxs + ", annParamIdxs=" + annParamIdxs + ']';

        return annParamIdxs.get(0);
    }

    /**
     * Create {@link GridifyRangeArgument} for task.
     *
     * @param arg Task argument contains all necessary data for method invoke.
     * @param input Input collection..
     * @return Argument for task.
     * @throws GridException In case of error.
     */
    public GridifyRangeArgument createTaskArgument(GridifyRangeArgument arg, Collection<?> input) throws GridException {
        GridifyRangeArgument res = new GridifyRangeArgument();

        res.setTarget(arg.getTarget());
        res.setMethodClass(arg.getMethodClass());
        res.setMethodName(arg.getMethodName());
        res.setMethodReturnType(arg.getMethodReturnType());
        res.setMethodParameterTypes(arg.getMethodParameterTypes());
        res.setParamIndex(arg.getParamIndex());

        Object[] mtdArgs = new Object[arg.getMethodParameters().length];

        System.arraycopy(arg.getMethodParameters(), 0, mtdArgs, 0, arg.getMethodParameters().length);

        res.setMethodParameters(mtdArgs);

        assert arg.getParamIndex() != -1;

        Class<?> paramCls = arg.getMethodParameterTypes()[arg.getParamIndex()];

        assert paramCls != null;

        Object paramValue = GridifyUtils.collectionToParameter(paramCls, input);

        if (paramValue == null)
            throw new GridException("Failed to create task argument for type: " + paramCls.getName());

        mtdArgs[arg.getParamIndex()] = paramValue;

        return res;
    }

    /**
     * Create {@link org.apache.ignite.compute.gridify.GridifyArgument} for job.
     *
     * @param arg Task argument contains all necessary data for method invoke.
     * @param input Input collection used in job.
     * @return Argument for job.
     * @throws GridException In case of error.
     */
    public GridifyArgument createJobArgument(GridifyRangeArgument arg, Collection<?> input) throws GridException {
        GridifyArgumentAdapter res = new GridifyArgumentAdapter();

        res.setTarget(arg.getTarget());
        res.setMethodClass(arg.getMethodClass());
        res.setMethodName(arg.getMethodName());
        res.setMethodParameterTypes(arg.getMethodParameterTypes());

        Object[] mtdArgs = new Object[arg.getMethodParameters().length];

        System.arraycopy(arg.getMethodParameters(), 0, mtdArgs, 0, arg.getMethodParameters().length);

        res.setMethodParameters(mtdArgs);

        assert arg.getParamIndex() != -1;

        Class<?> paramCls = arg.getMethodParameterTypes()[arg.getParamIndex()];

        assert paramCls != null;

        Object paramValue = GridifyUtils.collectionToParameter(paramCls, input);

        if (paramValue == null)
            throw new GridException("Failed to create job argument for type: " + paramCls.getName());

        mtdArgs[arg.getParamIndex()] = paramValue;

        return res;
    }
}

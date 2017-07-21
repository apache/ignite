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

package org.apache.ignite.internal.util.gridify;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.compute.gridify.GridifyInput;
import org.apache.ignite.compute.gridify.GridifySetToSet;
import org.apache.ignite.compute.gridify.GridifySetToValue;
import org.apache.ignite.compute.gridify.aop.GridifyArgumentAdapter;

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
     * @throws IgniteCheckedException In case of error.
     */
    public GridifyRangeArgument createTaskArgument(GridifyRangeArgument arg, Collection<?> input) throws IgniteCheckedException {
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
            throw new IgniteCheckedException("Failed to create task argument for type: " + paramCls.getName());

        mtdArgs[arg.getParamIndex()] = paramValue;

        return res;
    }

    /**
     * Create {@link org.apache.ignite.compute.gridify.GridifyArgument} for job.
     *
     * @param arg Task argument contains all necessary data for method invoke.
     * @param input Input collection used in job.
     * @return Argument for job.
     * @throws IgniteException In case of error.
     */
    public GridifyArgument createJobArgument(GridifyRangeArgument arg, Collection<?> input) throws IgniteException {
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
            throw new IgniteException("Failed to create job argument for type: " + paramCls.getName());

        mtdArgs[arg.getParamIndex()] = paramValue;

        return res;
    }
}
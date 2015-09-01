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

package org.apache.ignite.compute.gridify.aop;

import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Convenience adapter for {@link org.apache.ignite.compute.gridify.GridifyArgument} interface. This adapter
 * should be used in custom grid job implementations.
 * <p>
 * See {@link org.apache.ignite.compute.gridify.Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see org.apache.ignite.compute.gridify.Gridify
 */
public class GridifyArgumentAdapter implements GridifyArgument {
    /** */
    private static final long serialVersionUID = 0L;

    /** Method class. */
    private Class<?> cls;

    /** Method name. */
    private String mtdName;

    /** Method parameter types. */
    private Class<?>[] types;

    /** Method parameters. */
    private Object[] params;

    /** Method execution state. */
    private Object target;

    /**
     * Empty constructor.
     */
    public GridifyArgumentAdapter() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param orig Copy to create this instance from.
     * @param newParams Optional array of new parameters to override the ondes from {@code orig}.
     */
    public GridifyArgumentAdapter(GridifyArgument orig, Object... newParams) {
        A.notNull(orig, "orig");

        cls = orig.getMethodClass();
        mtdName = orig.getMethodName();
        target = orig.getTarget();

        types = new Class[orig.getMethodParameterTypes().length];
        params = new Object[orig.getMethodParameters().length];

        System.arraycopy(orig.getMethodParameters(), 0, params, 0, params.length);
        System.arraycopy(orig.getMethodParameterTypes(), 0, types, 0, types.length);

        // Override parameters, if any.
        if (newParams.length > 0)
            setMethodParameters(newParams);
    }

    /**
     * Creates a fully initialized gridify argument.
     *
     * @param cls Method class.
     * @param mtdName Method name.
     * @param types Method parameter types.
     * @param params Method parameters.
     * @param target Target object.
     */
    public GridifyArgumentAdapter(Class<?> cls, String mtdName, Class<?>[] types, Object[] params, Object target) {
        this.cls = cls;
        this.mtdName = mtdName;
        this.types = types;
        this.params = params;
        this.target = target;
    }

    /** {@inheritDoc} */
    @Override public Class<?> getMethodClass() {
        return cls;
    }

    /** {@inheritDoc} */
    @Override public String getMethodName() {
        return mtdName;
    }

    /** {@inheritDoc} */
    @Override public Class<?>[] getMethodParameterTypes() {
        return types;
    }

    /** {@inheritDoc} */
    @Override public Object[] getMethodParameters() {
        return params;
    }

    /**
     * Sets method class.
     *
     * @param cls Method class.
     */
    public void setMethodClass(Class<?> cls) {
        this.cls = cls;
    }

    /**
     * Sets method name.
     *
     * @param mtdName Method name.
     */
    public void setMethodName(String mtdName) {
        this.mtdName = mtdName;
    }

    /**
     * Sets method parameter types.
     *
     * @param types Method parameter types.
     */
    public void setMethodParameterTypes(Class<?>... types) {
        this.types = types;
    }

    /**
     * Updates parameter type.
     *
     * @param type Parameter type to set.
     * @param index Index of the parameter.
     */
    public void updateMethodParameterType(Class<?> type, int index) {
        types[index] = type;
    }

    /**
     * Sets method parameters.
     *
     * @param params Method parameters.
     */
    public void setMethodParameters(Object... params) {
        this.params = params;
    }

    /**
     * Updates method parameter.
     *
     * @param param Method parameter value to set.
     * @param index Parameter's index.
     */
    public void updateMethodParameter(Object param, int index) {
        params[index] = param;
    }

    /**
     * Sets target object for method execution.
     *
     * @param target Target object for method execution.
     */
    public void setTarget(Object target) {
        this.target = target;
    }

    /**
     * Gets target object for method execution.
     *
     * @return Target object for method execution.
     */
    @Override public Object getTarget() {
        return target;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridifyArgumentAdapter.class, this);
    }
}
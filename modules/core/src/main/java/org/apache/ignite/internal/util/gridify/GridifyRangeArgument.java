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

import java.util.Iterator;
import org.apache.ignite.compute.gridify.aop.GridifyArgumentAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Convenience adapter for {@link org.apache.ignite.compute.gridify.GridifyArgument} interface.
 * This adapter used in grid task for {@link org.apache.ignite.compute.gridify.GridifySetToSet} and
 * {@link org.apache.ignite.compute.gridify.GridifySetToValue} annotations.
 * <p>
 * See {@link org.apache.ignite.compute.gridify.Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see org.apache.ignite.compute.gridify.GridifySetToValue
 * @see org.apache.ignite.compute.gridify.GridifySetToSet
 */
public class GridifyRangeArgument extends GridifyArgumentAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Identify where to find data in method signature. */
    private int paramIdx = -1;

    /** Method return type. */
    private Class<?> mtdReturnType;

    /**
     * Gets index identifies where to find data in method signature.
     *
     * @return Index.
     */
    public int getParamIndex() {
        return paramIdx;
    }

    /**
     * Sets index identifies where to find data in method signature.
     *
     * @param paramIdx Index.
     */
    public void setParamIndex(int paramIdx) {
        this.paramIdx = paramIdx;
    }

    /**
     * Gets method return type in the same order they appear in method
     * signature.
     *
     * @return Method return type.
     */
    public Class<?> getMethodReturnType() {
        return mtdReturnType;
    }

    /**
     * Sets method return type.
     *
     * @param mtdReturnType Method return type.
     */
    public void setMethodReturnType(Class<?> mtdReturnType) {
        this.mtdReturnType = mtdReturnType;
    }

    /**
     * Returns elements {@link Iterator} for current input argument.
     * @return Iterator.
     */
    public Iterator<?> getInputIterator() {
        return GridifyUtils.getIterator(getMethodParameters()[paramIdx]);
    }

    /**
     * Returns elements size for current input argument or
     * {@link GridifyUtils#UNKNOWN_SIZE} for unknown input size.
     *
     * @return Elements size for current input argument or {@link GridifyUtils#UNKNOWN_SIZE}.
     */
    public int getInputSize() {
        return GridifyUtils.getLength(getMethodParameters()[paramIdx]);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridifyRangeArgument.class, this);
    }
}
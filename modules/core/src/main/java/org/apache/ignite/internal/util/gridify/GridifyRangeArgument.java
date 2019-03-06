/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
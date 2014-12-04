/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.gridify;

import org.apache.ignite.compute.gridify.aop.*;
import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.*;

/**
 * Convenience adapter for {@link GridifyArgument} interface.
 * This adapter used in grid task for {@link GridifySetToSet} and
 * {@link GridifySetToValue} annotations.
 * <p>
 * See {@link Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see GridifySetToValue
 * @see GridifySetToSet
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

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Defines generic reducer that collects multiple values and reduces them into one.
 * Reducers are useful in computations when results from multiple remote jobs need
 * to be reduced into one, e.g. {@link GridCompute#call(Collection, GridReducer)} method.
 *
 * @param <E> Type of collected values.
 * @param <R> Type of reduced value.
 */
public abstract class GridReducer<E, R> extends GridLambdaAdapter {
    /**
     * Collects given value. If this method returns {@code false} then {@link #reduce()}
     * will be called right away. Otherwise caller will continue collecting until all
     * values are processed.
     *
     * @param e Value to collect.
     * @return {@code true} to continue collecting, {@code false} to instruct caller to stop
     *      collecting and call {@link #reduce()} method.
     */
    public abstract boolean collect(@Nullable E e);

    /**
     * Reduces collected values into one.
     *
     * @return Reduced value.
     */
    public abstract R reduce();
}

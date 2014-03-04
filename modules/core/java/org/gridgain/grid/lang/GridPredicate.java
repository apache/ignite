/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.lang.*;

/**
 * Defines a predicate which accepts a parameter and returns {@code true} or {@code false}. In
 * GridGain, predicates are generally used for filtering nodes within grid projections, or for
 * providing atomic filters when performing cache operation, like in
 * {@link GridCache#put(Object, Object, GridPredicate[])} method.
 *
 * @param <E> Type of predicate parameter.
 */
public abstract class GridPredicate<E> extends GridLambdaAdapter {
    /**
     * Predicate body.
     *
     * @param e Predicate parameter.
     * @return Return value.
     */
    public abstract boolean apply(E e);
}

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

/**
 * Defines generic closure with one parameter. Closure is a simple executable which accepts a parameter and
 * returns a value.
 * <p>
 * In GridGain closures are mainly used for executing distributed computations
 * on the grid, like in {@link GridCompute#apply(GridClosure, Object)} method.
 *
 * @param <E> Type of closure parameter.
 * @param <R> Type of the closure return value.
 */
public abstract class GridClosure<E, R> extends GridLambdaAdapter {
    /**
     * Closure body.
     *
     * @param e Closure parameter.
     * @return Closure return value.
     */
    public abstract R apply(E e);
}

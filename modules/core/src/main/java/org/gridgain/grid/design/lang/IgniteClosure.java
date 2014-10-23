/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.lang;

import org.gridgain.grid.compute.*;

import java.io.*;

/**
 * Defines generic closure with one parameter. Closure is a simple executable which accepts a parameter and
 * returns a value.
 * <p>
 * In GridGain closures are mainly used for executing distributed computations
 * on the grid, like in {@link GridCompute#apply(IgniteClosure, Object)} method.
 *
 * @param <E> Type of closure parameter.
 * @param <R> Type of the closure return value.
 */
public interface IgniteClosure<E, R> extends Serializable {
    /**
     * Closure body.
     *
     * @param e Closure parameter.
     * @return Closure return value.
     */
    public R apply(E e);
}

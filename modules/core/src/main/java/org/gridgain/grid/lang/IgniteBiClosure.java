/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import java.io.*;

/**
 * Defines generic closure with two parameters. Bi-Closure is a simple executable which
 * accepts two parameters and returns a value.
 *
 * @param <E1> Type of the first parameter.
 * @param <E2> Type of the second parameter.
 * @param <R> Type of the closure's return value.
 */
public interface IgniteBiClosure<E1, E2, R> extends Serializable {
    /**
     * Closure body.
     *
     * @param e1 First parameter.
     * @param e2 Second parameter.
     * @return Closure return value.
     */
    public abstract R apply(E1 e1, E2 e2);
}

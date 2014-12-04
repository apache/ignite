/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.lang;

import java.io.*;

/**
 * Closure with two in-parameters and void return type.
 *
 * @param <E1> Type of the first parameter.
 * @param <E2> Type of the second parameter.
 */
public interface IgniteBiInClosure<E1, E2> extends Serializable {
    /**
     * Closure body.
     *
     * @param e1 First parameter.
     * @param e2 Second parameter.
     */
    public void apply(E1 e1, E2 e2);
}

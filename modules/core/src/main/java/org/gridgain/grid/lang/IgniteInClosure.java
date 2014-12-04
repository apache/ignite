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
 * Closure with one in-parameter and void return type.
 *
 * @param <E> Type of closure argument.
 */
public interface IgniteInClosure<E> extends Serializable {
    /**
     * Closure body.
     *
     * @param e Closure argument.
     */
    public void apply(E e);
}

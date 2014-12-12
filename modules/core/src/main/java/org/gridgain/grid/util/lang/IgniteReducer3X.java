/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Convenient reducer subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method and
 * properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 * @see RX3
 */
public abstract class IgniteReducer3X<E1, E2, E3, R> implements IgniteReducer3<E1, E2, E3, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public R apply() {
        try {
            return applyx();
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /**
     * Reducer body that can throw {@link IgniteCheckedException}.
     *
     * @return Reducer return value.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the reducer.
     */
    public abstract R applyx() throws IgniteCheckedException;
}

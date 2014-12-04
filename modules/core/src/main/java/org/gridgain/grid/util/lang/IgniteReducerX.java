/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Convenient reducer subclass that allows for thrown grid exception. This class
 * implements {@link #reduce()} method that calls {@link #applyx()} method and
 * properly wraps {@link GridException} into {@link GridClosureException} instance.
 * @see RX1
 */
public abstract class IgniteReducerX<E1, R> implements IgniteReducer<E1, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public R reduce() {
        try {
            return applyx();
        }
        catch (GridException e) {
            throw F.wrap(e);
        }
    }

    /**
     * Reducer body that can throw {@link GridException}.
     *
     * @return Reducer return value.
     * @throws GridException Thrown in case of any error condition inside of the reducer.
     */
    public abstract R applyx() throws GridException;
}

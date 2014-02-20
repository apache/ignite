// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Convenient mapper subclass that allows for thrown grid exception. This class
 * implements {@link #apply(Object)} method that calls {@link #applyx(Object)} method
 * and properly wraps {@link GridException} into {@link GridClosureException} instance.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridMapperX<T1, T2> extends GridMapper<T1, T2> {
    /** {@inheritDoc} */
    @Override public T2 apply(T1 t1) {
        try {
            return applyx(t1);
        }
        catch (GridException e) {
            throw F.wrap(e);
        }
    }

    /**
     * Mapper body that can throw {@link GridException}.
     *
     * @param t1 The variable the closure is called or closed on.
     * @return Optional return value.
     * @throws GridException Thrown in case of any error condition inside of the closure.
     */
    public abstract T2 applyx(T1 t1) throws GridException;
}

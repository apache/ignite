/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Convenient abs-closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method
 * and properly wraps {@link GridException} into {@link GridClosureException} instance.
 */
public abstract class GridAbsClosureX extends GridAbsClosure {
    /** {@inheritDoc} */
    @Override public void apply() {
        try {
            applyx();
        }
        catch (GridException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Closure body that can throw {@link GridException}.
     *
     * @throws GridException Thrown in case of any error condition inside of the closure.
     */
    public abstract void applyx() throws GridException;
}

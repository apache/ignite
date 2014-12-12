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
 * Convenient abs-closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method
 * and properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 */
public abstract class GridAbsClosureX extends GridAbsClosure {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void apply() {
        try {
            applyx();
        }
        catch (IgniteCheckedException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Closure body that can throw {@link IgniteCheckedException}.
     *
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract void applyx() throws IgniteCheckedException;
}

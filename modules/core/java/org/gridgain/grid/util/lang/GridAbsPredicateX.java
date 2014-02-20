// @java.file.header

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
 * Convenient predicate subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method
 * and properly wraps {@link GridException} into {@link GridClosureException} instance.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridAbsPredicateX extends GridAbsPredicate {
    /** {@inheritDoc} */
    @Override public boolean apply() {
        try {
            return applyx();
        }
        catch (GridException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Predicate body that can throw {@link GridException}.
     *
     * @return Return value.
     * @throws GridException Thrown in case of any error condition inside of the predicate.
     */
    public abstract boolean applyx() throws GridException;
}

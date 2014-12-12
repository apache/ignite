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
 * Convenient predicate subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method
 * and properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 */
public abstract class GridAbsPredicateX implements GridAbsPredicate {
    /** {@inheritDoc} */
    @Override public boolean apply() {
        try {
            return applyx();
        }
        catch (IgniteCheckedException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Predicate body that can throw {@link IgniteCheckedException}.
     *
     * @return Return value.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the predicate.
     */
    public abstract boolean applyx() throws IgniteCheckedException;
}

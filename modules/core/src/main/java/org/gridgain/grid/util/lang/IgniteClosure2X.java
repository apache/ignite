/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Convenient closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply(Object, Object)} method that calls {@link #applyx(Object, Object)}
 * method and properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 * @see CX2
 */
public abstract class IgniteClosure2X<E1, E2, R> implements IgniteBiClosure<E1, E2, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public R apply(E1 e1, E2 e2) {
        try {
            return applyx(e1, e2);
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /**
     * Closure body that can throw {@link IgniteCheckedException}.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called or closed on.
     * @param e2 Second bound free variable, i.e. the element the closure is called or closed on.
     * @return Optional return value.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract R applyx(E1 e1, E2 e2) throws IgniteCheckedException;
}

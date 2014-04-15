/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.*;

/**
 * This exception provides closures with facility to throw exceptions. Closures can't
 * throw checked exception and this class provides a standard idiom on how to wrap and pass an
 * exception up the call chain. It is also frequently used with {@link GridEither} to return
 * either wrapping exception or value from the closure.
 * @see GridEither
 * @see GridFunc#wrap(Throwable)
 */
public class GridClosureException extends GridRuntimeException {
    private static final long serialVersionUID = 6375296386989572774L;

    /**
     * Creates wrapper closure exception for given {@link GridException}.
     *
     * @param e Exception to wrap.
     */
    public GridClosureException(Throwable e) {
        super(e);
    }

    /**
     * Unwraps the original {@link Throwable} instance.
     *
     * @return The original {@link Throwable} instance.
     */
    public Throwable unwrap() {
        return getCause();
    }
}

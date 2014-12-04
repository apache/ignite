/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.gridgain.grid.util.lang.*;

/**
 * Defines {@code alias} for {@link org.gridgain.grid.util.lang.IgniteReducerX} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.gridgain.grid.util.lang.IgniteReducerX}.
 * @param <E1> Type of the free variable, i.e. the element the closure is called or closed on.
 * @param <R> Type of the closure's return value.
 * @see GridFunc
 * @see org.gridgain.grid.util.lang.IgniteReducerX
 */
public abstract class RX1<E1, R> extends IgniteReducerX<E1, R> {
    /** */
    private static final long serialVersionUID = 0L;
 /* No-op. */ }

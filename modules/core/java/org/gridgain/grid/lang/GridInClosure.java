// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.lang.*;

/**
 * Defines a convenient {@code side-effect only} closure, i.e. the closure that has {@code void} return type.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 *
 * @author @java.author
 * @version @java.version
 * @param <E1> Type of the free variable, i.e. the element the closure is called or closed on.
 * @see GridFunc
 */
public abstract class GridInClosure<E1> extends GridLambdaAdapter {
    /**
     * In-closure body.
     *
     * @param t Bound free variable, i.t. the element the closure is called or closed on.
     */
    public abstract void apply(E1 t);
}

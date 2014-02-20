// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Defines a convenient {@code side-effect only} closure, i.e. the closure that has {@code void} return type.
 * Since Java 6 doesn't provide a language construct for first-class function the closures are
 * implemented as interfaces.
 * <h2 class="header">Type Alias</h2>
 * To provide for more terse code you can use a typedef {@link org.gridgain.grid.util.typedef.CI2}
 * class or various factory methods in {@link GridFunc} class. Note, however, that since typedefs
 * in Java rely on inheritance you should not use these type aliases in signatures.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 *
 * @author @java.author
 * @version @java.version
 * @param <E1> Type of the first free variable, i.e. the element the closure is called or closed on.
 * @param <E2> Type of the second free variable, i.e. the element the closure is called or closed on.
 * @see C2
 * @see GridFunc
 */
public abstract class GridBiInClosure<E1, E2> extends GridLambdaAdapter {
    /**
     * Closure body.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called or closed on.
     * @param e2 Second bound free variable, i.e. the element the closure is called or closed on.
     */
    public abstract void apply(E1 e1, E2 e2);
}

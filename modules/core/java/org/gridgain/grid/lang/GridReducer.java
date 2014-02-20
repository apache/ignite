// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

/**
 * Defines generic {@code for-all} or {@code reduce} type of closure. Unlike {@code for-each} type of closure
 * that returns optional value on each execution of the closure - the reducer returns a single
 * value for one or more collected values.
 * <p>
 * Closures are a first-class functions that are defined with
 * (or closed over) their free variables that are bound to the closure scope at execution. Since
 * Java 6 doesn't provide a language construct for first-class function the closures are implemented
 * as abstract classes.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 *
 * @author @java.author
 * @version @java.version
 * @param <E1> Type of the free variable, i.e. the element the closure is called on.
 * @param <R> Type of the closure's return value.
 */
public abstract class GridReducer<E1, R> extends GridLambdaAdapter {
    /**
     * Collects given value. All values will be reduced by {@link #apply()} method.
     * <p>
     * The {@code null}s could be passed if the data being collected is indeed {@code null}.
     * If execution failed this method will not be called.
     *
     * @param e Value to collect.
     * @return {@code true} to continue collecting, {@code false} to instruct caller to stop
     *      collecting and call {@link #apply()} method.
     */
    public abstract boolean collect(@Nullable E1 e);

    /**
     * Factory closure body.
     *
     * @return Element.
     */
    public abstract R apply();
}

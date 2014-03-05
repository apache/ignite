/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Defines generic {@code for-each} type of closure. Closure is a first-class function that is defined with
 * (or closed over) its free variables that are bound to the closure scope at execution. Since
 * Java 6 doesn't provide a language construct for first-class function the closures are implemented
 * as abstract classes.
 * <h2 class="header">Type Alias</h2>
 * To provide for more terse code you can use a typedef {@link C3} class or various factory methods in
 * {@link GridFunc} class. Note, however, that since typedefs in Java rely on inheritance you should
 * not use these type aliases in signatures.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 * @param <E1> Type of the first free variable, i.e. the element the closure is called or closed on.
 * @param <E2> Type of the second free variable, i.e. the element the closure is called or closed on.
 * @param <E3> Type of the third free variable, i.e. the element the closure is called or closed on.
 * @param <R> Type of the closure's return value.
 * @see C3
 * @see GridFunc
 */
public abstract class GridClosure3<E1, E2, E3, R> extends GridLambdaAdapter {
    /**
     * Closure body.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called or closed on.
     * @param e2 Second bound free variable, i.e. the element the closure is called or closed on.
     * @param e3 Third bound free variable, i.e. the element the closure is called or closed on.
     * @return Optional return value.
     */
    public abstract R apply(E1 e1, E2 e2, E3 e3);

    /**
     * Curries this closure with given value. When result closure is called it will
     * be executed with given value.
     *
     * @param e1 Value to curry with.
     * @return Curried or partially applied closure with given value.
     */
    public GridBiClosure<E2, E3, R> curry(final E1 e1) {
        return new C2<E2, E3, R>() {
            {
                peerDeployLike(GridClosure3.this);
            }

            @Override public R apply(E2 e2, E3 e3) {
                return GridClosure3.this.apply(e1, e2, e3);
            }
        };
    }

    /**
     * Curries this closure with given value. When result closure is called it will
     * be executed with given value.
     *
     * @param e1 Value to curry with.
     * @param e2 Value to curry with.
     * @return Curried or partially applied closure with given value.
     */
    public GridClosure<E3, R> curry(final E1 e1, final E2 e2) {
        return new C1<E3, R>() {
            {
                peerDeployLike(GridClosure3.this);
            }

            @Override public R apply(E3 e3) {
                return GridClosure3.this.apply(e1, e2, e3);
            }
        };
    }

    /**
     * Curries this closure with given values. When result closure is called it will
     * be executed with given values.
     *
     * @param e1 Value to curry with.
     * @param e2 Value to curry with.
     * @param e3 Value to curry with.
     * @return Curried or partially applied closure with given values.
     */
    public GridOutClosure<R> curry(final E1 e1, final E2 e2, final E3 e3) {
        return new CO<R>() {
            {
                peerDeployLike(GridClosure3.this);
            }

            @Override public R apply() {
                return GridClosure3.this.apply(e1, e2, e3);
            }
        };
    }

    /**
     * Gets closure that applies given closure over the result of {@code this} closure.
     *
     * @param c Closure.
     * @param <A> Return type of new closure.
     * @return New closure.
     */
    public <A> GridClosure3<E1, E2, E3, A> andThen(final GridClosure<R, A> c) {
        return new GridClosure3<E1, E2, E3, A>() {
            @Override public A apply(E1 e1, E2 e2, E3 e3) {
                return c.apply(GridClosure3.this.apply(e1, e2, e3));
            }
        };
    }

    /**
     * Gets closure that applies given closure over the result of {@code this} closure.
     *
     * @param c Closure.
     * @return New closure.
     */
    public GridInClosure3<E1, E2, E3> andThen(final GridInClosure<R> c) {
        return new GridInClosure3<E1, E2, E3>() {
            @Override public void apply(E1 e1, E2 e2, E3 e3) {
                c.apply(GridClosure3.this.apply(e1, e2, e3));
            }
        };
    }
}

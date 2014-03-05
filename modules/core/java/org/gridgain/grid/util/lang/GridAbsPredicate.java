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
 * Defines absolute (no-arg) predicate construct. Predicate like closure is a first-class function
 * that is defined with (or closed over) its free variables that are bound to the closure
 * scope at execution.
 * <h2 class="header">Type Alias</h2>
 * To provide for more terse code you can use a typedef {@link P1} class or various factory methods in
 * {@link GridFunc} class. Note, however, that since typedefs in Java rely on inheritance you should
 * not use these type aliases in signatures.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 * @see P1
 * @see GridFunc
 */
public abstract class GridAbsPredicate extends GridLambdaAdapter {
    /**
     * Predicate body.
     *
     * @return Return value.
     */
    public abstract boolean apply();

    /**
     * Gets predicate that ignores its argument and returns the same value as this
     * predicate.
     *
     * @param <E> Type of ignore argument.
     * @return Predicate that ignores its argument and returns the same value as this
     *      predicate.
     */
    public <E> GridPredicate<E> uncurry() {
        GridPredicate<E> p = new P1<E>() {
            @Override public boolean apply(E e) {
                return GridAbsPredicate.this.apply();
            }
        };

        p.peerDeployLike(this);

        return p;
    }

    /**
     * Gets predicate that ignores its arguments and returns the same value as this
     * predicate.
     *
     * @param <E1> Type of 1st ignore argument.
     * @param <E2> Type of 2nd ignore argument.
     * @return Predicate that ignores its arguments and returns the same value as this
     *      predicate.
     */
    public <E1, E2> GridBiPredicate<E1, E2> uncurry2() {
        GridBiPredicate<E1, E2> p = new P2<E1, E2>() {
            @Override public boolean apply(E1 e1, E2 e2) {
                return GridAbsPredicate.this.apply();
            }
        };

        p.peerDeployLike(this);

        return p;
    }

    /**
     * Gets predicate that ignores its arguments and returns the same value as this
     * predicate.
     *
     * @param <E1> Type of 1st ignore argument.
     * @param <E2> Type of 2nd ignore argument.
     * @param <E3> Type of 3d ignore argument.
     * @return Predicate that ignores its arguments and returns the same value as this
     *      predicate.
     */
    public <E1, E2, E3> GridPredicate3<E1, E2, E3> uncurry3() {
        GridPredicate3<E1, E2, E3> p = new P3<E1, E2, E3>() {
            @Override public boolean apply(E1 e1, E2 e2, E3 e3) {
                return GridAbsPredicate.this.apply();
            }
        };

        p.peerDeployLike(this);

        return p;
    }

    /**
     * Gets closure that applies given closure over the result of {@code this} predicate.
     *
     * @param c Closure.
     * @param <A> Return type of new closure.
     * @return New closure.
     */
    public <A> GridOutClosure<A> andThen(final GridClosure<Boolean, A> c) {
        return new GridOutClosure<A>() {
            @Override public A apply() {
                return c.apply(GridAbsPredicate.this.apply());
            }
        };
    }

    /**
     * Gets closure that applies given closure over the result of {@code this} predicate.
     *
     * @param c Closure.
     * @return New closure.
     */
    public GridAbsClosure andThen(final GridInClosure<Boolean> c) {
        return new GridAbsClosure() {
            @Override public void apply() {
                c.apply(GridAbsPredicate.this.apply());
            }
        };
    }

    /**
     * Gets predicate that applies given predicate over the result of {@code this} predicate.
     *
     * @param c Predicate.
     * @return New predicate.
     */
    public GridAbsPredicate andThen(final GridPredicate<Boolean> c) {
        return new GridAbsPredicate() {
            @Override public boolean apply() {
                return c.apply(GridAbsPredicate.this.apply());
            }
        };
    }
}

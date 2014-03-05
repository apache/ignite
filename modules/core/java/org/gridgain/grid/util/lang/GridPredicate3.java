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
 * Defines predicate construct. Predicate like closure is a first-class function
 * that is defined with (or closed over) its free variables that are bound to the closure
 * scope at execution.
 * <p>
 * This form of predicate is essentially a syntactic "sugar" providing shorter syntax for:
 * <pre name="code" class="java">
 * ...
 * GridPredicate&lt;GridTuple3&lt;E1, E2, E3&gt;&gt;
 * ...
 * </pre>
 * <h2 class="header">Type Alias</h2>
 * To provide for more terse code you can use a typedef {@link P3} class or various factory methods in
 * {@link GridFunc} class. Note, however, that since typedefs in Java rely on inheritance you should
 * not use these type aliases in signatures.
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 * @param <E1> Type of the first free variable, i.e. the element the closure is called on.
 * @param <E2> Type of the second free variable, i.e. the element the closure is called on.
 * @param <E3> Type of the third free variable, i.e. the element the closure is called on.
 * @see P3
 * @see GridFunc
 */
public abstract class GridPredicate3<E1, E2, E3> {
    /**
     * Predicate body.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called or closed on.
     * @param e2 Second bound free variable, i.e. the element the closure is called or closed on.
     * @param e3 Third bound free variable, i.e. the element the closure is called or closed on.
     * @return Return value.
     */
    public abstract boolean apply(E1 e1, E2 e2, E3 e3);

    /**
     * Gets closure that applies given closure over the result of {@code this} predicate.
     *
     * @param c Closure.
     * @param <A> Return type of new closure.
     * @return New closure.
     */
    public <A> GridClosure3<E1, E2, E3, A> andThen(final GridClosure<Boolean, A> c) {
        return new GridClosure3<E1, E2, E3, A>() {
            @Override public A apply(E1 e1, E2 e2, E3 e3) {
                return c.apply(GridPredicate3.this.apply(e1, e2, e3));
            }
        };
    }

    /**
     * Gets closure that applies given closure over the result of {@code this} predicate.
     *
     * @param c Closure.
     * @return New closure.
     */
    public GridInClosure3<E1, E2, E3> andThen(final GridInClosure<Boolean> c) {
        return new GridInClosure3<E1, E2, E3>() {
            @Override public void apply(E1 e1, E2 e2, E3 e3) {
                c.apply(GridPredicate3.this.apply(e1, e2, e3));
            }
        };
    }

    /**
     * Gets predicate that applies given predicate over the result of {@code this} predicate.
     *
     * @param c Predicate.
     * @return New predicate.
     */
    public GridPredicate3<E1, E2, E3> andThen(final GridPredicate<Boolean> c) {
        return new GridPredicate3<E1, E2, E3>() {
            @Override public boolean apply(E1 e1, E2 e2, E3 e3) {
                return c.apply(GridPredicate3.this.apply(e1, e2, e3));
            }
        };
    }
}

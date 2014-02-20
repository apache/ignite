// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
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
 * @param <E1> Type of the first free variable, i.e. the element the closure is called on.
 * @param <E2> Type of the second free variable, i.e. the element the closure is called on.
 * @param <R> Type of the closure's return value.
 */
public abstract class GridReducer2<E1, E2, R> extends GridOutClosure<R> {
    /**
     * Collects given values. All values will be reduced by {@link #apply()} method.
     * <p>
     * The {@code null}s could be passed if the data being collected is indeed {@code null}.
     * If execution failed this method will not be called.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called on.
     * @param e2 Second bound free variable, i.e. the element the closure is called on.
     * @return {@code true} to continue collecting, {@code false} to instruct caller to stop
     *      collecting and call {@link #apply()} method.
     */
    public abstract boolean collect(@Nullable E1 e1, @Nullable E2 e2);

    /**
     * Curries this closure with given tuple values. When result closure is called it will
     * be executed with given values.
     *
     * @param t Tuples each with two values.
     * @return Curried or partially applied closure with given values.
     */
    public GridOutClosure<R> curry(final GridBiTuple<E1, E2>... t) {
        GridOutClosure<R> rdc = new CO<R>() {
            @Override public R apply() {
                for (GridBiTuple<E1, E2> p : t) {
                    collect(p.get1(), p.get2());
                }

                return GridReducer2.this.apply();
            }
        };

        rdc.peerDeployLike(this);

        return rdc;
    }

    /**
     * Gets reducer that ignores third argument passed to method
     * {@link GridReducer3#collect(Object, Object, Object)} and works as this reducer
     * with just first and second arguments.
     * <p>
     * NOTE: the name is changed from the standard one to avoid signature conflict on erased
     * return type.
     *
     * @param <E3> Type of 3d argument that is ignored.
     * @return Reducer that ignores third argument passed to method
     *      {@link GridReducer3#collect(Object, Object, Object)} and works as this reducer
     *      with first and second arguments.
     */
    public <E3> GridReducer3<E1, E2, E3, R> uncurry3x() {
        GridReducer3<E1, E2, E3, R> rdc = new R3<E1, E2, E3, R>() {
            @Override public boolean collect(E1 e1, E2 e2, E3 e3) {
                return GridReducer2.this.collect(e1, e2);
            }

            @Override public R apply() {
                return GridReducer2.this.apply();
            }
        };

        rdc.peerDeployLike(this);

        return rdc;
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Internal utility class that contains not peer-deployable
 * predicates for use in internal logic.
 *
 * @author @java.author
 * @version @java.version
 */
public class F0 {
    /**
     * Negates given predicates.
     * <p>
     * Gets predicate (not peer-deployable) that evaluates to {@code true} if any of given predicates
     * evaluates to {@code false}. If all predicates evaluate to {@code true} the
     * result predicate will evaluate to {@code false}.
     *
     * @param p Predicate to negate.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Negated predicate (not peer-deployable).
     */
    public static <T> GridPredicate<T> not(@Nullable final GridPredicate<? super T>... p) {
        return F.isAlwaysFalse(p) ? F.<T>alwaysTrue() : F.isAlwaysTrue(p) ? F.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                return !F.isAll(t, p);
            }
        };
    }

    /**
     * Gets predicate (not peer-deployable) that evaluates to {@code true} if its free variable is not equal
     * to {@code target} or both are {@code null}.
     *
     * @param target Object to compare free variable to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate (not peer-deployable) that evaluates to {@code true} if its free variable is not equal
     *      to {@code target} or both are {@code null}.
     */
    public static <T> GridPredicate<T> notEqualTo(@Nullable final T target) {
        return new P1<T>() {
            @Override public boolean apply(T t) {
                return !F.eq(t, target);
            }
        };
    }

    /**
     * Gets predicate (not peer-deployable) that returns {@code true} if its free variable
     * is not contained in given collection.
     *
     * @param c Collection to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      collection elements.
     * @return Predicate (not peer-deployable) that returns {@code true} if its free variable is not
     *      contained in given collection.
     */
    public static <T> GridPredicate<T> notIn(@Nullable final Collection<? extends T> c) {
        return F.isEmpty(c) ? GridFunc.<T>alwaysTrue() : new P1<T>() {
            @Override public boolean apply(T t) {
                assert c != null;

                return !c.contains(t);
            }
        };
    }

    /**
     * Gets predicate (not perr-deployable) that evaluates to {@code true} if its free variable is equal
     * to {@code target} or both are {@code null}.
     *
     * @param target Object to compare free variable to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is equal to
     *      {@code target} or both are {@code null}.
     */
    public static <T> GridPredicate<T> equalTo(@Nullable final T target) {
        return new P1<T>() {
            @Override public boolean apply(T t) {
                return F.eq(t, target);
            }
        };
    }

    /**
     * Get a predicate (non peer-deployable) that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param p1 Passed in predicates.
     * @param p2 Passed in predicates.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> GridPredicate<T> and(@Nullable final GridPredicate<? super T>[] p1,
        @Nullable final GridPredicate<? super T>... p2) {
        if (F.isAlwaysFalse(p1) || F.isAlwaysFalse(p2))
            return F.alwaysFalse();

        if (F.isAlwaysTrue(p1) && F.isAlwaysTrue(p2))
            return F.alwaysTrue();

        final boolean e1 = F.isEmpty(p1);
        final boolean e2 = F.isEmpty(p2);

        if (e1 && e2)
            return F.alwaysTrue();

        if (e1 && !e2) {
            assert p2 != null;

            if (p2.length == 1)
                return (GridPredicate<T>)p2[0];
        }

        if (!e1 && e2) {
            assert p1 != null;

            if (p1.length == 1)
                return (GridPredicate<T>)p1[0];
        }

        if ((e1 || isAllNodePredicates(p1)) && (e2 || isAllNodePredicates(p2))) {
            Set<UUID> ids = new GridLeanSet<>();

            if (!e1) {
                assert p1 != null;

                for (GridPredicate<? super T> p : p1)
                    ids.addAll(((GridNodePredicate)p).nodeIds());
            }

            if (!e2) {
                assert p2 != null;

                for (GridPredicate<? super T> p : p2)
                    ids.addAll(((GridNodePredicate)p).nodeIds());
            }

            // T must be <T extends GridNode>.
            return (GridPredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    if (!e1) {
                        assert p1 != null;

                        for (GridPredicate<? super T> p : p1)
                            if (p != null && !p.apply(t))
                                return false;
                    }

                    if (!e2) {
                        assert p2 != null;

                        for (GridPredicate<? super T> p : p2)
                            if (p != null && !p.apply(t))
                                return false;
                    }

                    return true;
                }
            };
        }
    }

    /**
     * Get a predicate (not peer-deployable) that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate. If none provided - always-{@code false} predicate is
     *      returned.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> GridPredicate<T> and(@Nullable final GridPredicate<? super T>... ps) {
        if (F.isEmpty(ps))
            return F.alwaysTrue();

        if (F.isAlwaysFalse(ps))
            return F.alwaysFalse();

        if (F.isAlwaysTrue(ps))
            return F.alwaysTrue();

        if (isAllNodePredicates(ps)) {
            assert ps != null;

            Set<UUID> ids = new GridLeanSet<>();

            for (GridPredicate<? super T> p : ps) {
                Collection<UUID> list = ((GridNodePredicate)p).nodeIds();

                if (ids.isEmpty())
                    ids.addAll(list);
                else
                    ids.retainAll(list);
            }

            // T must be <T extends GridNode>.
            return (GridPredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    assert ps != null;

                    for (GridPredicate<? super T> p : ps)
                        if (p != null && !p.apply(t))
                            return false;

                    return true;
                }
            };
        }
    }

    /**
     * Gets predicate (not peer-deployable) that returns {@code true} if its free variable is contained
     * in given collection.
     *
     * @param c Collection to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      collection elements.
     * @return Predicate (not peer-deployable) that returns {@code true} if its free variable is
     *      contained in given collection.
     */
    public static <T> GridPredicate<T> in(@Nullable final Collection<? extends T> c) {
        return F.isEmpty(c) ? GridFunc.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                assert c != null;

                return c.contains(t);
            }
        };
    }

    /**
     * Provides predicate (not peer-deployable) which returns {@code true} if it receives an element
     * that is contained in the passed in collection.
     *
     * @param c Collection used for predicate filter.
     * @param <T> Element type.
     * @return Predicate which returns {@code true} if it receives an element
     *  that is contained in the passed in collection.
     */
    public static <T> GridPredicate<T> contains(@Nullable final Collection<T> c) {
        return c == null || c.isEmpty() ? GridFunc.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                return c.contains(t);
            }
        };
    }

    /**
     * Provides predicate (not peer-deployable) which returns {@code true} if it receives an element
     * that is not contained in the passed in collection.
     *
     * @param c Collection used for predicate filter.
     * @param <T> Element type.
     * @return Predicate which returns {@code true} if it receives an element
     *  that is not contained in the passed in collection.
     */
    public static <T> GridPredicate<T> notContains(@Nullable final Collection<T> c) {
        return c == null || c.isEmpty() ? GridFunc.<T>alwaysTrue() : new P1<T>() {
            @Override public boolean apply(T t) {
                return !c.contains(t);
            }
        };
    }

    /**
     * Tests if all passed in predicates are instances of {@link GridNodePredicate} class.
     *
     * @param ps Collection of predicates to test.
     * @return {@code True} if all passed in predicates are instances of {@link GridNodePredicate} class.
     */
    public static boolean isAllNodePredicates(@Nullable Iterable<? extends GridPredicate<?>> ps) {
        if (F.isEmpty(ps))
            return false;

        assert ps != null;

        for (GridPredicate<?> p : ps)
            if (!(p instanceof GridNodePredicate))
                return false;

        return true;
    }

    /**
     * Tests if all passed in predicates are instances of {@link GridNodePredicate} class.
     *
     * @param ps Collection of predicates to test.
     * @return {@code True} if all passed in predicates are instances of {@link GridNodePredicate} class.
     */
    public static boolean isAllNodePredicates(@Nullable GridPredicate<?>... ps) {
        if (F.isEmpty(ps))
            return false;

        assert ps != null;

        for (GridPredicate<?> p : ps)
            if (!(p instanceof GridNodePredicate))
                return false;

        return true;
    }

    /**
     * Creates map with given values, adding a strict not-null check for value.
     *
     * @param key Key.
     * @param val Value.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K key, V val) {
        A.notNull(val, "val");

        return F.asMap(key, val);
    }
}

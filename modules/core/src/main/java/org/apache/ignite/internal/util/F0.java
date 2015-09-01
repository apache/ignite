/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheEntrySerializablePredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.GridNodePredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Internal utility class that contains not peer-deployable
 * predicates for use in internal logic.
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
    public static <T> IgnitePredicate<T> not(@Nullable final IgnitePredicate<? super T>... p) {
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
    public static <T> IgnitePredicate<T> notEqualTo(@Nullable final T target) {
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
    public static <T> IgnitePredicate<T> notIn(@Nullable final Collection<? extends T> c) {
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
    public static <T> IgnitePredicate<T> equalTo(@Nullable final T target) {
        return new P1<T>() {
            @Override public boolean apply(T t) {
                return F.eq(t, target);
            }
        };
    }

    /**
     * @param p1 Filter1.
     * @param p2 Filter2.
     * @return And filter.
     */
    public static CacheEntryPredicate and0(@Nullable final CacheEntryPredicate[] p1,
        @Nullable final CacheEntryPredicate... p2) {
        if (CU.isAlwaysFalse0(p1) || CU.isAlwaysFalse0(p2))
            return CU.alwaysFalse0();

        if (CU.isAlwaysTrue0(p1) && CU.isAlwaysTrue0(p2))
            return CU.alwaysTrue0();

        final boolean e1 = F.isEmpty(p1);
        final boolean e2 = F.isEmpty(p2);

        if (e1 && e2)
            return CU.alwaysTrue0();

        if (e1 && !e2) {
            assert p2 != null;

            if (p2.length == 1)
                return p2[0];
        }

        if (!e1 && e2) {
            assert p1 != null;

            if (p1.length == 1)
                return p1[0];
        }

        return new CacheEntrySerializablePredicate(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                if (!e1) {
                    assert p1 != null;

                    for (CacheEntryPredicate p : p1)
                        if (p != null && !p.apply(e))
                            return false;
                }

                if (!e2) {
                    assert p2 != null;

                    for (CacheEntryPredicate p : p2)
                        if (p != null && !p.apply(e))
                            return false;
                }

                return true;
            }

            @Override public void entryLocked(boolean locked) {
                if (p1 != null) {
                    for (CacheEntryPredicate p : p1) {
                        if (p != null)
                            p.entryLocked(locked);
                    }
                }

                if (p2 != null) {
                    for (CacheEntryPredicate p : p2) {
                        if (p != null)
                            p.entryLocked(locked);
                    }
                }
            }

            @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
                if (!e1) {
                    assert p1 != null;

                    for (CacheEntryPredicate p : p1)
                        p.prepareMarshal(ctx);
                }

                if (!e2) {
                    assert p2 != null;

                    for (CacheEntryPredicate p : p2)
                        p.prepareMarshal(ctx);
                }
            }
        });
    }

    /**
     * @param p Filter1.
     * @param ps Filter2.
     * @return And filter.
     */
    public static CacheEntryPredicate and0(
        @Nullable final CacheEntryPredicate p,
        @Nullable final CacheEntryPredicate... ps) {
        if (p == null && F.isEmptyOrNulls(ps))
            return CU.alwaysTrue0();

        if (F.isAlwaysFalse(p) && F.isAlwaysFalse(ps))
            return CU.alwaysFalse0();

        if (F.isAlwaysTrue(p) && F.isAlwaysTrue(ps))
            return CU.alwaysTrue0();

        return new CacheEntrySerializablePredicate(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                assert ps != null;

                if (p != null && !p.apply(e))
                    return false;

                for (CacheEntryPredicate p : ps) {
                    if (p != null && !p.apply(e))
                        return false;
                }

                return true;
            }

            @Override public void entryLocked(boolean locked) {
                assert ps != null;

                if (p != null)
                    p.entryLocked(locked);

                for (CacheEntryPredicate p : ps) {
                    if (p != null)
                        p.entryLocked(locked);
                }
            }

            @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
                assert ps != null;

                if (p != null)
                    p.prepareMarshal(ctx);

                for (CacheEntryPredicate p : ps)
                    if (p != null)
                        p.prepareMarshal(ctx);
            }
        });
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
    public static <T> IgnitePredicate<T> and(@Nullable final IgnitePredicate<? super T>[] p1,
        @Nullable final IgnitePredicate<? super T>... p2) {
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
                return (IgnitePredicate<T>)p2[0];
        }

        if (!e1 && e2) {
            assert p1 != null;

            if (p1.length == 1)
                return (IgnitePredicate<T>)p1[0];
        }

        if ((e1 || isAllNodePredicates(p1)) && (e2 || isAllNodePredicates(p2))) {
            Set<UUID> ids = new GridLeanSet<>();

            if (!e1) {
                assert p1 != null;

                for (IgnitePredicate<? super T> p : p1)
                    ids.addAll(((GridNodePredicate)p).nodeIds());
            }

            if (!e2) {
                assert p2 != null;

                for (IgnitePredicate<? super T> p : p2)
                    ids.addAll(((GridNodePredicate)p).nodeIds());
            }

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    if (!e1) {
                        assert p1 != null;

                        for (IgnitePredicate<? super T> p : p1)
                            if (p != null && !p.apply(t))
                                return false;
                    }

                    if (!e2) {
                        assert p2 != null;

                        for (IgnitePredicate<? super T> p : p2)
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
    public static <T> IgnitePredicate<T> and(
        @Nullable final IgnitePredicate<? super T> p,
        @Nullable final IgnitePredicate<? super T>... ps
    ) {
        if (p == null && F.isEmptyOrNulls(ps))
            return F.alwaysTrue();

        if (F.isAlwaysFalse(p) && F.isAlwaysFalse(ps))
            return F.alwaysFalse();

        if (F.isAlwaysTrue(p) && F.isAlwaysTrue(ps))
            return F.alwaysTrue();

        if (isAllNodePredicates(p) && isAllNodePredicates(ps)) {
            assert ps != null;

            Set<UUID> ids = new GridLeanSet<>();

            for (IgnitePredicate<? super T> p0 : ps) {
                Collection<UUID> list = ((GridNodePredicate)p0).nodeIds();

                if (ids.isEmpty())
                    ids.addAll(list);
                else
                    ids.retainAll(list);
            }

            Collection<UUID> list = ((GridNodePredicate)p).nodeIds();

            if (ids.isEmpty())
                ids.addAll(list);
            else
                ids.retainAll(list);

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    assert ps != null;

                    if (p != null && !p.apply(t))
                        return false;

                    for (IgnitePredicate<? super T> p : ps)
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
    public static <T> IgnitePredicate<T> in(@Nullable final Collection<? extends T> c) {
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
    public static <T> IgnitePredicate<T> contains(@Nullable final Collection<T> c) {
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
    public static <T> IgnitePredicate<T> notContains(@Nullable final Collection<T> c) {
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
    public static boolean isAllNodePredicates(@Nullable Iterable<? extends IgnitePredicate<?>> ps) {
        if (F.isEmpty(ps))
            return false;

        assert ps != null;

        for (IgnitePredicate<?> p : ps)
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
    public static boolean isAllNodePredicates(@Nullable IgnitePredicate<?>... ps) {
        if (F.isEmpty(ps))
            return false;

        assert ps != null;

        for (IgnitePredicate<?> p : ps)
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
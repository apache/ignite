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
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Internal utility class that contains not peer-deployable
 * predicates for use in internal logic.
 */
@SuppressWarnings("deprecation")
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
    @SuppressWarnings("unchecked")
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
/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.lang.gridfunc;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Defines a predicate which accepts a parameter and always returns {@code false}
 *
 * @param <E> Type of predicate parameter.
 */
public class AlwaysFalsePredicate<E> implements IgnitePredicate<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Predicate body.
     *
     * @param e Predicate parameter.
     * @return Always <code>false</code>
     */
    @Override public boolean apply(E e) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AlwaysFalsePredicate.class, this);
    }
}

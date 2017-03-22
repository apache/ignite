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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Compound predicate which evaluates to {@code true} if all nested predicates evaluated to {@code true}.
 */
public class CompoundPredicate<E> implements IgnitePredicate<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Internal predicates. */
    private IgnitePredicate<E>[] predicates;

    /**
     * Constructor.
     *
     * @param predicates Predicates.
     */
    public CompoundPredicate(IgnitePredicate<E>[] predicates) {
        A.notNull(predicates, "predicates");

        for (int i = 0; i < predicates.length; i++) {
            if (predicates[i] == null)
                throw new NullPointerException("Predicate cannot be null [idx=" + i + ']');
        }

        this.predicates = predicates;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(E e) {
        for (IgnitePredicate<E> predicate : predicates)
            if (!predicate.apply(e))
                return false;

        return true;
    }
}

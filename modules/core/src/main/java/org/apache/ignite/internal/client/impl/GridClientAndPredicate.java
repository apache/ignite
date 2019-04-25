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

package org.apache.ignite.internal.client.impl;

import org.apache.ignite.internal.client.GridClientPredicate;

/**
 * AND predicate. Passes if and only if both provided filters accept the node.
 * This filter uses short-term condition evaluation, i.e. second filter would not
 * be invoked if first filter returned {@code false}.
 */
class GridClientAndPredicate<T> implements GridClientPredicate<T> {
    /** First filter to check. */
    private GridClientPredicate<? super T> first;

    /** Second filter to check. */
    private GridClientPredicate<? super T> second;

    /**
     * Creates AND filter.
     *
     * @param first First filter to check.
     * @param second Second filter to check.
     */
    GridClientAndPredicate(GridClientPredicate<? super T> first, GridClientPredicate<? super T> second) {
        assert first != null;
        assert second != null;

        this.first = first;
        this.second = second;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T elem) {
        return first.apply(elem) && second.apply(elem);
    }
}
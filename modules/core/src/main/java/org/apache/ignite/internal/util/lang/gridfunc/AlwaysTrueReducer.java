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
import org.apache.ignite.lang.IgniteReducer;

/**
 * Reducer which always returns {@code true} from {@link org.apache.ignite.lang.IgniteReducer#collect(Object)}
 *
 * @param <T> Reducer element type.
 */
public class AlwaysTrueReducer<T> implements IgniteReducer<T, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final T elem;

    /**
     * @param elem Element to return from {@link org.apache.ignite.lang.IgniteReducer#reduce()} method.
     */
    public AlwaysTrueReducer(T elem) {
        this.elem = elem;
    }

    /** {@inheritDoc} */
    @Override public boolean collect(T e) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public T reduce() {
        return elem;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AlwaysTrueReducer.class, this);
    }
}

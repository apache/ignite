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

package org.apache.ignite.internal.util.future;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Compound future with reducer which accepts and produces results of the same type.
 */
public class GridCompoundIdentityFuture<T> extends GridCompoundFuture<T, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public GridCompoundIdentityFuture() {
        // No-op.
    }

    /**
     * @param rdc Reducer.
     */
    public GridCompoundIdentityFuture(@Nullable IgniteReducer<T, T> rdc) {
        super(rdc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCompoundIdentityFuture.class, this, super.toString());
    }
}
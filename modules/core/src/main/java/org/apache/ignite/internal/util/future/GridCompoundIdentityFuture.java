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

package org.apache.ignite.internal.util.future;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Future composed of multiple inner futures.
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
     * @param ctx Context.
     * @param rdc Reducer.
     */
    public GridCompoundIdentityFuture(GridKernalContext ctx, @Nullable IgniteReducer<T, T> rdc) {
        super(rdc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCompoundIdentityFuture.class, this, super.toString());
    }
}
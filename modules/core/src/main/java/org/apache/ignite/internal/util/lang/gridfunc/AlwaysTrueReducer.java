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

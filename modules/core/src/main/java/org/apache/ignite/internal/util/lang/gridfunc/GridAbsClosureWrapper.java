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

import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Wraps given closure.
 *
 * @param <T> Input type.
 */
public class GridAbsClosureWrapper<T> extends GridAbsClosure {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteInClosure<? super T> closure;

    /** */
    private final T closureArgument;

    /**
     * @param closure Closure.
     * @param closureArgument Closure argument.
     */
    public GridAbsClosureWrapper(IgniteInClosure<? super T> closure, T closureArgument) {
        this.closure = closure;
        this.closureArgument = closureArgument;
    }

    /** {@inheritDoc} */
    @Override public void apply() {
        closure.apply(closureArgument);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAbsClosureWrapper.class, this);
    }
}

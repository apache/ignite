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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.CX3;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Convenient closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply(Object, Object, Object)} method that calls
 * {@link #applyx(Object, Object, Object)} method and properly wraps {@link IgniteCheckedException}
 * into {@link GridClosureException} instance.
 * @see CX3
 */
public abstract class GridClosure3X<E1, E2, E3, R> implements GridClosure3<E1, E2, E3, R> {
    /** {@inheritDoc} */
    @Override public R apply(E1 e1, E2 e2, E3 e3) {
        try {
            return applyx(e1, e2, e3);
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /**
     * Closure body that can throw {@link IgniteCheckedException}.
     *
     * @param e1 First bound free variable, i.e. the element the closure is called or closed on.
     * @param e2 Second bound free variable, i.e. the element the closure is called or closed on.
     * @param e3 Third bound free variable, i.e. the element the closure is called or closed on.
     * @return Optional return value.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract R applyx(E1 e1, E2 e2, E3 e3) throws IgniteCheckedException;
}
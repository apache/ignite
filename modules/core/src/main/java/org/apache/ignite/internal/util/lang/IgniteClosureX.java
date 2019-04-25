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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Convenient closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply(Object)} method that calls {@link #applyx(Object)} method
 * and properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 * @see CX1
 */
public abstract class IgniteClosureX<E, R> implements IgniteClosure<E, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public R apply(E e) {
        try {
            return applyx(e);
        }
        catch (IgniteCheckedException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Closure body that can throw {@link IgniteCheckedException}.
     *
     * @param e The variable the closure is called or closed on.
     * @return Optional return value.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract R applyx(E e) throws IgniteCheckedException;
}
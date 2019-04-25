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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;

/**
 * Convenient in-closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply(Object, Object, Object)} method that calls
 * {@link #applyx(Object, Object, Object)} method and properly wraps {@link IgniteCheckedException}
 * into {@link GridClosureException} instance.
 * @see CIX3
 */
public abstract class GridInClosure3X<E1, E2, E3> implements GridInClosure3<E1, E2, E3> {
    /** {@inheritDoc} */
    @Override public void apply(E1 e1, E2 e2, E3 e3) {
        try {
            applyx(e1, e2, e3);
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /**
     * In-closure body that can throw {@link IgniteCheckedException}.
     *
     * @param e1 First variable the closure is called or closed on.
     * @param e2 Second variable the closure is called or closed on.
     * @param e3 Third variable the closure is called or closed on.
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract void applyx(E1 e1, E2 e2, E3 e3) throws IgniteCheckedException;
}
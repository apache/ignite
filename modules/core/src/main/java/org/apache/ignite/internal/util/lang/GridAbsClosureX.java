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
import org.apache.ignite.internal.util.typedef.F;

/**
 * Convenient abs-closure subclass that allows for thrown grid exception. This class
 * implements {@link #apply()} method that calls {@link #applyx()} method
 * and properly wraps {@link IgniteCheckedException} into {@link GridClosureException} instance.
 */
public abstract class GridAbsClosureX extends GridAbsClosure {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void apply() {
        try {
            applyx();
        }
        catch (IgniteCheckedException ex) {
            throw F.wrap(ex);
        }
    }

    /**
     * Closure body that can throw {@link IgniteCheckedException}.
     *
     * @throws IgniteCheckedException Thrown in case of any error condition inside of the closure.
     */
    public abstract void applyx() throws IgniteCheckedException;
}
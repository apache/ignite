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

package org.apache.ignite.internal.util.typedef;

import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.IgniteClosureX;

/**
 * Defines {@code alias} for {@link org.apache.ignite.internal.util.lang.IgniteClosureX} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.apache.ignite.internal.util.lang.IgniteClosureX}.
 * @param <E1> Type of the free variable, i.e. the element the closure is called or closed on.
 * @param <R> Type of the closure's return value.
 * @see GridFunc
 * @see org.apache.ignite.internal.util.lang.IgniteClosureX
 */
public abstract class CX1<E1, R> extends IgniteClosureX<E1, R> {
    /** */
    private static final long serialVersionUID = 0L;
 /* No-op. */ }
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
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;

/**
 * Defines {@code alias} for {@link org.apache.ignite.internal.util.lang.IgniteOutClosureX} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.apache.ignite.internal.util.lang.IgniteOutClosureX}.
 * @param <T> Type of the factory closure.
 * @see GridFunc
 * @see org.apache.ignite.internal.util.lang.IgniteOutClosureX
 */
public abstract class COX<T> extends IgniteOutClosureX<T> {
    /** */
    private static final long serialVersionUID = 0L;
 /* No-op. */ }
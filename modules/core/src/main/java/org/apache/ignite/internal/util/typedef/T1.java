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

package org.apache.ignite.internal.util.typedef;

import java.io.Externalizable;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.GridTuple;

/**
 * Defines {@code alias} for {@link GridTuple} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link GridTuple}.
 * @param <V> Type of the free variable.
 * @see GridFunc
 * @see GridTuple
 */
public class T1<V> extends GridTuple<V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public T1() {
        // No-op.
    }

    /**
     * Constructs mutable object with given value.
     *
     * @param val Wrapped value.
     */
    public T1(V val) {
        super(val);
    }
}
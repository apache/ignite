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
import org.apache.ignite.internal.util.lang.GridTuple6;

/**
 * Defines {@code alias} for {@link GridTuple6} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link GridTuple6}.
 * @see GridFunc
 * @see GridTuple
 */
public class T6<V1, V2, V3, V4, V5, V6> extends GridTuple6<V1, V2, V3, V4, V5, V6> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public T6() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param v1 First value.
     * @param v2 Second value.
     * @param v3 Third value.
     * @param v4 Forth value.
     * @param v5 Fifth value.
     * @param v6 Sixth value.
     */
    public T6(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5, V6 v6) {
        super(v1, v2, v3, v4, v5, v6);
    }
}
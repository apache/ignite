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

import java.io.Externalizable;
import org.jetbrains.annotations.Nullable;

/**
 * Simple extension over {@link GridTuple3} for three objects of the same type.
 */
public class GridTriple<T> extends GridTuple3<T, T, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridTriple() {
        // No-op.
    }

    /**
     * Creates triple with given objects.
     *
     * @param t1 First object in triple.
     * @param t2 Second object in triple.
     * @param t3 Third object in triple.
     */
    public GridTriple(@Nullable T t1, @Nullable T t2, @Nullable T t3) {
        super(t1, t2, t3);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"CloneDoesntDeclareCloneNotSupportedException"})
    @Override public Object clone() {
        return super.clone();
    }
}
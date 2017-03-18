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

import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Predicate that evaluates to {@code true} if its free variable is instance of the given class.
 *
 * @param <T> Type of the free variable, i.e. the element the predicate is called on.
 */
public class IgnitePredicateIsAssignableFrom<T> implements P1<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Class<?> cls;

    /**
     * @param cls Class to compare to.
     */
    public IgnitePredicateIsAssignableFrom(Class<?> cls) {
        this.cls = cls;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T t) {
        return t != null && cls.isAssignableFrom(t.getClass());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgnitePredicateIsAssignableFrom.class, this);
    }
}

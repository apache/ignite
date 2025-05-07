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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Defines a predicate which checks a parameter on <code>null</code>.
 *
 * @param <E> Type of predicate parameter.
 */
public class IsNotNullPredicate<E> implements IgnitePredicate<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param e Parameter for check.
     * @return 'true' if parameter NOT equals to <code>null</code>, otherwise 'false'.
     */
    @Override public boolean apply(E e) {
        return e != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IsNotNullPredicate.class, this);
    }
}

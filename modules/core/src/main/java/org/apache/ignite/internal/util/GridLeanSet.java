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

package org.apache.ignite.internal.util;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Lean set implementation. Internally this set is based on {@link GridLeanMap}.
 *
 * @see GridLeanMap
 *
 */
public class GridLeanSet<E> extends GridSetWrapper<E> implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new, empty set with a default initial capacity,
     * load factor, and concurrencyLevel.
     */
    public GridLeanSet() {
        super(new GridLeanMap<E, Object>());
    }

    /**
     * Constructs lean set with initial size.
     *
     * @param size Initial size.
     */
    public GridLeanSet(int size) {
        super(new GridLeanMap<E, Object>(size));
    }

    /**
     * Creates a new set with the same elements as the given collection. The
     * collection is created with a capacity of twice the number of mappings in
     * the given map or 11 (whichever is greater), and a default load factor
     * and concurrencyLevel.
     *
     * @param c Collection to add.
     */
    public GridLeanSet(Collection<E> c) {
        super(new GridLeanMap<>(F.zip(c, VAL)));
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public Object clone() {
        try {
            GridLeanSet<E> clone = (GridLeanSet<E>)super.clone();

            clone.map = (Map<E, Object>)((GridLeanMap)map).clone();

            return clone;
        }
        catch (CloneNotSupportedException ignore) {
            throw new InternalError();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridLeanSet.class, this, "elements", map.keySet());
    }
}
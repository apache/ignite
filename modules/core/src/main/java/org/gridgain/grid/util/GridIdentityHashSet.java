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

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Set counterpart for {@link IdentityHashMap}.
 */
public class GridIdentityHashSet<E> extends GridSetWrapper<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates default identity hash set.
     */
    public GridIdentityHashSet() {
        super(new IdentityHashMap<E, Object>());
    }

    /**
     * Creates identity hash set of given size.
     *
     * @param size Start size for the set.
     */
    public GridIdentityHashSet(int size) {
        super(new IdentityHashMap<E, Object>(size));

        A.ensure(size >= 0, "size >= 0");
    }

    /**
     * Creates identity has set initialized given collection.
     *
     * @param vals Values to initialize.
     */
    public GridIdentityHashSet(Collection<E> vals) {
        super(F.isEmpty(vals) ? new IdentityHashMap<E, Object>(0) : new IdentityHashMap<E, Object>(vals.size()), vals);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIdentityHashSet.class, this, super.toString());
    }
}

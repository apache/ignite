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
 *
 */

package org.apache.ignite.internal.util;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Read-only wrapper over multiple sets.
 */
public class PartitionedReadOnlySet<T> extends AbstractSet<T> {
    /** */
    private final Collection<Set<T>> sets;

    /**
     * Constructor.
     * @param sets Internal sets.
     */
    public PartitionedReadOnlySet(Collection<Set<T>> sets) {
        this.sets = sets;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        Collection<Iterator<T>> iterators = new ArrayList<>(sets.size());

        for (Set<T> set : sets)
            iterators.add(set.iterator());

        return F.flatIterators(iterators);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int size = 0;

        for (Set<T> set : sets)
            size += set.size();

        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        for (Set<T> set : sets)
            if (set.contains(o))
                return true;

        return false;
    }
}

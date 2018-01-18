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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.jetbrains.annotations.NotNull;

/**
 * Page ID storage implementation without any striping. Backward compatible implementation.
 */
public class PagesGridConcurrentHashSet implements Set<FullPageId>, PageIdCollection {
    /** Delegate, actual pages are stored in this set. */
    private Set<FullPageId> set = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<FullPageId> iterator() {
        return set.iterator();
    }


    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        return set.toArray();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T[] toArray(@NotNull T[] a) {
        return set.<T>toArray(a);
    }

    /** {@inheritDoc} */
    @Override public boolean add(FullPageId id) {
        return set.add(id);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return set.size();
    }


    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return set.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return set.contains(o);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return set.remove(o);
    }


    /** {@inheritDoc} */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        return set.containsAll(c);
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(@NotNull Collection<? extends FullPageId> c) {
        return set.addAll(c);
    }


    /** {@inheritDoc} */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        return set.retainAll(c);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        return set.removeAll(c);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        set.clear();
    }
}

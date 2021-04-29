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
package org.apache.ignite.internal.commandline.indexreader;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Stores all index items.
 */
class ItemsListStorage<T> implements ItemStorage<T> {
    private final List<T> store = new LinkedList<>();

    /** {@inheritDoc} */
    @Override public void add(T item) {
        store.add(item);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(T item) {
        throw new UnsupportedOperationException("'contains' operation is not supported by ItemsListStorage.");
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return store.size();
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        return store.iterator();
    }
}
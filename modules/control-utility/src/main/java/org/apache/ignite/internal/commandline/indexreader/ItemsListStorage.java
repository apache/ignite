/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.indexreader;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * Stores all index items.
 */
class ItemsListStorage<T> implements ItemStorage<T> {
    final List<T> store = new LinkedList<>();

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

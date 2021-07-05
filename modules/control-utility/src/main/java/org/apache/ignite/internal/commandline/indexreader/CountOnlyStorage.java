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

import org.jetbrains.annotations.NotNull;

/**
 * Imitates item storage, but stores only items count. It can be used when storing each item is not necessary.
 */
class CountOnlyStorage<T> implements ItemStorage<T> {
    /** */
    private long size;

    /** {@inheritDoc} */
    @Override public void add(T item) {
        size++;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(T item) {
        throw new UnsupportedOperationException("'contains' operation is not supported by SizeOnlyStorage.");
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return size;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Iteration is not supported by SizeOnlyStorage.");
    }
}

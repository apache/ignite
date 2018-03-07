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

import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;

/**
 * List that contains only one element. By default the list contains null element.
 */
public class MutableSingletonList<E> extends AbstractList<E> {
    /** The only element of collection. */
    private E element;

    /** {@inheritDoc} */
    @Override public E get(int index) {
        if (index != 0)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());

        return element;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        add(0, e);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void add(int index, E element) {
        if (index != 0)
            throw new IllegalStateException("Element already added to singleton list");
        else
            this.element = element;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 1;
    }

    /**
     * Creates filled singleton list.
     *
     * @return singleton list
     */
    @SuppressWarnings("unchecked")
    public Collection<E> singletonList() {
        return Collections.singletonList(element);
    }
}

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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Set that contains only one element. By default the set contains null element.
 */
public class MutableSingletonSet<E> extends AbstractSet<E> {
    /** The only element of set. */
    private E element;

    /** {@inheritDoc} */
    @Override public void clear() {
        element = null;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        element = e;

        return true;
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return new Iterator<E>() {

            private boolean hasNext = true;

            @Override public boolean hasNext() {
                return hasNext;
            }

            @Override public void remove() {
                element = null;
            }

            @Override public E next() {
                if (hasNext) {
                    hasNext = false;

                    return element;
                }

                throw new NoSuchElementException();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 1;
    }

    /**
     * Creates filled singleton set.
     *
     * @return singleton set
     */
    @SuppressWarnings("unchecked")
    public Set<E> singletonSet() {
        return Collections.singleton(element);
    }
}

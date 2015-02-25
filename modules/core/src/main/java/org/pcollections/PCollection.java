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

package org.pcollections;

import java.util.*;

/**
 * An immutable, persistent collection of non-null elements of type E.
 *
 * @param <E>
 * @author harold
 */
public interface PCollection<E> extends Collection<E> {
    /**
     * @param e non-null
     * @return a collection which contains e and all of the elements of this
     */
    public PCollection<E> plus(E e);

    /**
     * @param list contains no null elements
     * @return a collection which contains all of the elements of list and this
     */
    public PCollection<E> plusAll(Collection<? extends E> list);

    /**
     * @param e
     * @return this with a single instance of e removed, if e is in this
     */
    public PCollection<E> minus(Object e);

    /**
     * @param list
     * @return this with all elements of list completely removed
     */
    public PCollection<E> minusAll(Collection<?> list);

    // TODO public PCollection<E> retainingAll(Collection<?> list);

    @Deprecated
    boolean add(E o);

    @Deprecated
    boolean remove(Object o);

    @Deprecated
    boolean addAll(Collection<? extends E> c);

    @Deprecated
    boolean removeAll(Collection<?> c);

    @Deprecated
    boolean retainAll(Collection<?> c);

    @Deprecated
    void clear();
}

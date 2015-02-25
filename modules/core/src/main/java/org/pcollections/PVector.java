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
 * An immutable, persistent list.
 *
 * @param <E>
 * @author harold
 */
public interface PVector<E> extends PSequence<E> {
    /**
     * Returns a vector consisting of the elements of this with e appended.
     */
    //@Override
    public PVector<E> plus(E e);

    /**
     * Returns a vector consisting of the elements of this with list appended.
     */
    //@Override
    public PVector<E> plusAll(Collection<? extends E> list);

    //@Override
    public PVector<E> with(int i, E e);

    //@Override
    public PVector<E> plus(int i, E e);

    //@Override
    public PVector<E> plusAll(int i, Collection<? extends E> list);

    //@Override
    public PVector<E> minus(Object e);

    //@Override
    public PVector<E> minusAll(Collection<?> list);

    //@Override
    public PVector<E> minus(int i);

    //@Override
    public PVector<E> subList(int start, int end);
}

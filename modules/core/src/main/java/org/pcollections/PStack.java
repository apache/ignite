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
 * An immutable, persistent stack.
 *
 * @param <E>
 * @author harold
 */
public interface PStack<E> extends PSequence<E> {
    /**
     * Returns a stack consisting of the elements of this with e prepended.
     */
    //@Override
    public PStack<E> plus(E e);

    /**
     * Returns a stack consisting of the elements of this with list prepended in reverse.
     */
    //@Override
    public PStack<E> plusAll(Collection<? extends E> list);

    //@Override
    public PStack<E> with(int i, E e);

    //@Override
    public PStack<E> plus(int i, E e);

    //@Override
    public PStack<E> plusAll(int i, Collection<? extends E> list);

    //@Override
    public PStack<E> minus(Object e);

    //@Override
    public PStack<E> minusAll(Collection<?> list);

    //@Override
    public PStack<E> minus(int i);

    //@Override
    public PStack<E> subList(int start, int end);

    /**
     * @param start
     * @return subList(start, this.size())
     */
    public PStack<E> subList(int start);
}

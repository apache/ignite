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
 * A persistent queue.
 *
 * @author mtklein
 */
public interface PQueue<E> extends PCollection<E>, Queue<E> {
    // TODO i think PQueue should extend PSequence,
    // even though the methods will be inefficient -- H

    /* Guaranteed to stay as a PQueue, i.e. guaranteed-fast methods */
    public PQueue<E> minus();

    public PQueue<E> plus(E e);

    public PQueue<E> plusAll(Collection<? extends E> list);


    /* May switch to other PCollection, i.e. may-be-slow methods */
    public PCollection<E> minus(Object e);

    public PCollection<E> minusAll(Collection<?> list);

    @Deprecated
    boolean offer(E o);

    @Deprecated
    E poll();

    @Deprecated
    E remove();
}

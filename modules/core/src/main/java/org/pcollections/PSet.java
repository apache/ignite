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
 * An immutable, persistent set, containing no duplicate elements.
 *
 * @param <E>
 * @author harold
 */
public interface PSet<E> extends PCollection<E>, Set<E> {
    //@Override
    public PSet<E> plus(E e);

    //@Override
    public PSet<E> plusAll(Collection<? extends E> list);

    //@Override
    public PSet<E> minus(Object e);

    //@Override
    public PSet<E> minusAll(Collection<?> list);
}

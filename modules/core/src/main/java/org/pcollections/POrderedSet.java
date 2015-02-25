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
 * Like {@link PSet} but preserves insertion order. Persistent equivalent of
 * {@link LinkedHashSet}.
 *
 * @param <E>
 * @author Tassilo Horn &lt;horn@uni-koblenz.de&gt;
 */
public interface POrderedSet<E> extends PSet<E> {

    public POrderedSet<E> plus(E e);

    public POrderedSet<E> plusAll(Collection<? extends E> list);

    public POrderedSet<E> minus(Object e);

    public POrderedSet<E> minusAll(Collection<?> list);

    E get(int index);

    int indexOf(Object o);
}

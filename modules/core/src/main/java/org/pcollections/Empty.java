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

/* Mike Klein, 2/27/2009 */

/* Empty remembers which classes implement the interface you want,
 * so you don't have to.
 */

/**
 * A static utility class for getting empty PCollections backed by the 'default'
 * implementations.
 *
 * @author mtklein
 */
public final class Empty {
    //non-instantiable:
    private Empty() {
    }

    public static <E> PStack<E> stack() {
        return ConsPStack.empty();
    }

    public static <E> PQueue<E> queue() {
        return AmortizedPQueue.empty();
    }

    public static <E> PVector<E> vector() {
        return TreePVector.empty();
    }

    public static <E> PSet<E> set() {
        return HashTreePSet.empty();
    }

    public static <E> POrderedSet<E> orderedSet() {
        return OrderedPSet.empty();
    }

    public static <E> PBag<E> bag() {
        return HashTreePBag.empty();
    }

    public static <K, V> PMap<K, V> map() {
        return HashTreePMap.empty();
    }
}

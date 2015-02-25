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
 * A static convenience class for creating efficient persistent bags.
 * <p/>
 * This class simply creates MapPBags backed by HashTreePMaps.
 *
 * @author harold
 */
public final class HashTreePBag {
    private static final MapPBag<Object> EMPTY
        = MapPBag.empty(HashTreePMap.<Object, Integer>empty());

    // not instantiable (or subclassable):
    private HashTreePBag() {
    }

    /**
     * @param <E>
     * @return an empty bag
     */
    @SuppressWarnings("unchecked")
    public static <E> MapPBag<E> empty() {
        return (MapPBag<E>) EMPTY;
    }

    /**
     * @param <E>
     * @param e
     * @return empty().plus(e)
     */
    public static <E> MapPBag<E> singleton(final E e) {
        return HashTreePBag.<E>empty().plus(e);
    }

    /**
     * @param <E>
     * @param list
     * @return empty().plusAll(map)
     */
    public static <E> MapPBag<E> from(final Collection<? extends E> list) {
        return HashTreePBag.<E>empty().plusAll(list);
    }
}

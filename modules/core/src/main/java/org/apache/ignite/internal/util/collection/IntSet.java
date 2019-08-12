/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.collection;

import java.util.Set;

/**
 *
 */
public interface IntSet extends Set<Integer> {
    /** Returns <tt>true</tt> if this set contains the specified element. */
    boolean contains(int element);

    /** Adds the specified element to this set. */
    boolean add(int element);

    /** Removes the specified element from this set. */
    boolean remove(int element);

    /** Returns array with primitive types **/
    int[] toIntArray();
}

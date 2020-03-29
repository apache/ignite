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

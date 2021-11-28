/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.List;

/**
 * Extend array list to add peek/poll first/last element.
 *
 * @param <E>
 */
public class ArrayDeque<E> extends java.util.ArrayList<E> {

    private static final long serialVersionUID = -4929318149975955629L;

    /**
     * Get the first element of list.
     */
    public static <E> E peekFirst(List<E> list) {
        return list.get(0);
    }

    /**
     * Remove the first element from list and return it.
     */
    public static <E> E pollFirst(List<E> list) {
        return list.remove(0);
    }

    /**
     * Get the last element of list.
     */
    public static <E> E peekLast(List<E> list) {
        return list.get(list.size() - 1);
    }

    /**
     * Remove the last element from list and return it.
     */
    public static <E> E pollLast(List<E> list) {
        return list.remove(list.size() - 1);
    }

    /**
     * Get the first element of list.
     */
    public E peekFirst() {
        return peekFirst(this);
    }

    /**
     * Get the last element of list.
     */
    public E peekLast() {
        return peekLast(this);
    }

    /**
     * Remove the first element from list and return it.
     */
    public E pollFirst() {
        return pollFirst(this);
    }

    /**
     * Remove the last element from list and return it.
     */
    public E pollLast() {
        return pollLast(this);
    }

    /**
     * Expose this methods so we not need to create a new subList just to remove a range of elements.
     *
     * Removes from this deque all of the elements whose index is between {@code fromIndex}, inclusive, and {@code
     * toIndex}, exclusive. Shifts any succeeding elements to the left (reduces their index). This call shortens the
     * deque by {@code (toIndex - fromIndex)} elements. (If {@code toIndex==fromIndex}, this operation has no effect.)
     *
     * @throws IndexOutOfBoundsException if {@code fromIndex} or {@code toIndex} is out of range ({@code fromIndex < 0
     * || fromIndex >= size() || toIndex > size() || toIndex < fromIndex})
     */
    @Override
    public void removeRange(int fromIndex, int toIndex) {
        super.removeRange(fromIndex, toIndex);
    }
}

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

package org.apache.ignite.internal.util;

/**
 * Pair of objects.
 *
 * @param <T> First object.
 * @param <V> Second object.
 */
public class Pair<T, V> {
    /** First obj. */
    private final T first;

    /** Second obj. */
    private final V second;

    /**
     * Constructor.
     *
     * @param first First object.
     * @param second Second object.
     */
    public Pair(T first, V second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return First object.
     */
    public T getFirst() {
        return first;
    }

    /**
     * @return Second object.
     */
    public V getSecond() {
        return second;
    }
}

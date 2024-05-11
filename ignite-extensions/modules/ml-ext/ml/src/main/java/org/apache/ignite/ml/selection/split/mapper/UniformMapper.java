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

package org.apache.ignite.ml.selection.split.mapper;

import java.io.Serializable;

/**
 * Interface for util mappers that maps a key-value pair to a point on the segment (0, 1).
 *
 * @param <K> Type of a key.
 * @param <V> Type of a value.
 */
@FunctionalInterface
public interface UniformMapper<K, V> extends Serializable {
    /**
     * Maps key-value pair to a point on the segment (0, 1).
     *
     * @param key Key.
     * @param val Value.
     * @return Point on the segment (0, 1).
     */
    public double map(K key, V val);
}

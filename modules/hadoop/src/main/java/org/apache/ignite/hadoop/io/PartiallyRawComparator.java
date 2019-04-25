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

package org.apache.ignite.hadoop.io;

/**
 * Partially raw comparator. Compares one deserialized value with serialized value.
 */
public interface PartiallyRawComparator<T> {
    /**
     * Do compare.
     *
     * @param val1 First value (deserialized).
     * @param val2Buf Second value (serialized).
     * @return A negative integer, zero, or a positive integer as this object is less than, equal to, or greater
     *     than the specified object.
     */
    int compare(T val1, RawMemory val2Buf);
}

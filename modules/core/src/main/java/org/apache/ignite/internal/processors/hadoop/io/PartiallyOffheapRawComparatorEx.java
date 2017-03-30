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

package org.apache.ignite.internal.processors.hadoop.io;

/**
 * Special version of raw comparator allowing direct access to the underlying memory.
 */
public interface PartiallyOffheapRawComparatorEx<T> {
    /**
     * Perform compare.
     *
     * @param val1 First value.
     * @param val2Ptr Pointer to the second value data.
     * @param val2Len Length of the second value data.
     * @return Result.
     */
    int compare(T val1, long val2Ptr, int val2Len);
}

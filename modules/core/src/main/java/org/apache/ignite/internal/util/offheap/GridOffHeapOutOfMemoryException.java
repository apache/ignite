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

package org.apache.ignite.internal.util.offheap;

/**
 * Thrown when memory could not be allocated.
 */
public class GridOffHeapOutOfMemoryException extends RuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs out of memory exception.
     *
     * @param size Size that could not be allocated.
     */
    public GridOffHeapOutOfMemoryException(long size) {
        super("Failed to allocate memory: " + size);
    }

    /**
     * Constructs out of memory exception.
     *
     * @param total Total allocated memory.
     * @param size Size that could not be allocated.
     */
    public GridOffHeapOutOfMemoryException(long total, long size) {
        super("Failed to allocate memory [total=" + total + ", failed=" + size + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
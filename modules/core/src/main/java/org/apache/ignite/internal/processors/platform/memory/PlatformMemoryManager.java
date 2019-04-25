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

package org.apache.ignite.internal.processors.platform.memory;

/**
 * Interop memory manager interface.
 */
public interface PlatformMemoryManager {
    /**
     * Allocates memory.
     *
     * @return Memory.
     */
    public PlatformMemory allocate();

    /**
     * Allocates memory having at least the given capacity.
     *
     * @param cap Minimum capacity.
     * @return Memory.
     */
    public PlatformMemory allocate(int cap);

    /**
     * Gets memory from existing pointer.
     *
     * @param memPtr Cross-platform memory pointer.
     * @return Memory.
     */
    public PlatformMemory get(long memPtr);
}
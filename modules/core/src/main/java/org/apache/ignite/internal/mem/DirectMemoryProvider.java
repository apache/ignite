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

package org.apache.ignite.internal.mem;

/**
 * Direct memory provider interface. Not thread-safe.
 */
public interface DirectMemoryProvider {
    /**
     * @param chunkSizes Initializes provider with the chunk sizes.
     */
    public void initialize(long[] chunkSizes);

    /**
     * Shuts down the provider.
     *
     * @param deallocate {@code True} to deallocate memory, {@code false} to allow memory reuse.
     */
    public void shutdown(boolean deallocate);

    /**
     * Attempts to allocate next memory region. Will return {@code null} if no more regions are available.
     *
     * @return Next memory region.
     */
    public DirectMemoryRegion nextRegion();
}

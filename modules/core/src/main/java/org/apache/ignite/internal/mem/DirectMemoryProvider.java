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
     * Shuts down the provider. Will deallocate all previously allocated regions.
     */
    public void shutdown();

    /**
     * Attempts to allocate next memory region. Will return {@code null} if no more regions are available.
     *
     * @return Next memory region.
     */
    public DirectMemoryRegion nextRegion();
}

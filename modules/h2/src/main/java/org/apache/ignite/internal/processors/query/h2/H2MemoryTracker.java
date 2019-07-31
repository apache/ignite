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

package org.apache.ignite.internal.processors.query.h2;

/**
 * Abstract memory tracker.
 */
public abstract class H2MemoryTracker implements AutoCloseable {
    /**
     * Reserve memory.
     *
     * @param size Memory to reserve in bytes.
     */
    public abstract void reserve(long size);

    /**
     * Release reserved memory.
     *
     * @param size Memory to release in bytes.
     */
    public abstract void release(long size);
}

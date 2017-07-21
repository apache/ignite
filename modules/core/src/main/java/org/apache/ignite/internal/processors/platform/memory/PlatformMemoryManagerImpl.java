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

package org.apache.ignite.internal.processors.platform.memory;

import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.flags;
import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.isExternal;
import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.isPooled;

/**
 * Interop memory manager implementation.
 */
public class PlatformMemoryManagerImpl implements PlatformMemoryManager {
    /** Native gateway. */
    private final PlatformCallbackGateway gate;

    /** Default allocation capacity. */
    private final int dfltCap;

    /** Thread-local pool. */
    private final ThreadLocal<PlatformMemoryPool> threadLocPool = new ThreadLocal<>();

    /**
     * Constructor.
     *
     * @param gate Native gateway.
     * @param dfltCap Default memory chunk capacity.
     */
    public PlatformMemoryManagerImpl(@Nullable PlatformCallbackGateway gate, int dfltCap) {
        this.gate = gate;
        this.dfltCap = dfltCap;
    }

    /** {@inheritDoc} */
    @Override public PlatformMemory allocate() {
        return allocate(dfltCap);
    }

    /** {@inheritDoc} */
    @Override public PlatformMemory allocate(int cap) {
        return pool().allocate(cap);
    }

    /** {@inheritDoc} */
    @Override public PlatformMemory get(long memPtr) {
        int flags = flags(memPtr);

        return isExternal(flags) ? new PlatformExternalMemory(gate, memPtr) :
            isPooled(flags) ? pool().get(memPtr) : new PlatformUnpooledMemory(memPtr);
    }

    /**
     * Gets or creates thread-local memory pool.
     *
     * @return Memory pool.
     */
    private PlatformMemoryPool pool() {
        PlatformMemoryPool pool = threadLocPool.get();

        if (pool == null) {
            pool = new PlatformMemoryPool();

            threadLocPool.set(pool);
        }

        return pool;
    }
}
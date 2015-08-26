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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.platform.callback.*;
import org.jetbrains.annotations.*;

/**
 * Interop external memory chunk.
 */
public class PlatformExternalMemory extends PlatformAbstractMemory {
    /** Native gateway. */
    private final PlatformCallbackGateway gate;

    /**
     * Constructor.
     *
     * @param gate Native gateway.
     * @param memPtr Memory pointer.
     */
    public PlatformExternalMemory(@Nullable PlatformCallbackGateway gate, long memPtr) {
        super(memPtr);

        this.gate = gate;
    }

    /** {@inheritDoc} */
    @Override public void reallocate(int cap) {
        if (gate == null)
            throw new IgniteException("Failed to re-allocate external memory chunk because it is read-only.");

        gate.memoryReallocate(memPtr, cap);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, memory must be released by native platform.
    }
}

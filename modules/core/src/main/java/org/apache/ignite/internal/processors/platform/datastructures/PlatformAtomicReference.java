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

package org.apache.ignite.internal.processors.platform.datastructures;

import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.processors.platform.memory.*;

/**
 * Platform atomic reference wrapper.
 */
public class PlatformAtomicReference extends PlatformAbstractTarget {
    /** */
    private final GridCacheAtomicReferenceImpl atomicRef;

    /**
     * Ctor.
     * @param ctx Context.
     * @param name Name.
     * @param memPtr Pointer to a stream with initial value. 0 for null initial value.
     * @param create Create flag.
     */
    public PlatformAtomicReference(PlatformContext ctx, String name, long memPtr, boolean create) {
        super(ctx);

        assert name != null;

        Object initVal = null;

        if (memPtr != 0) {
            try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
                initVal = platformCtx.reader(mem).readObjectDetached();
            }
        }

        atomicRef = (GridCacheAtomicReferenceImpl)ctx.kernalContext().grid().atomicReference(name, initVal, create);
    }

    public boolean isClosed() {
        return atomicRef.removed();
    }
    public void close() {
        atomicRef.close();
    }
}

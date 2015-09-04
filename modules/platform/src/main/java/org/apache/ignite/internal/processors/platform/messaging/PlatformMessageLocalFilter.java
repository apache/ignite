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

package org.apache.ignite.internal.processors.platform.messaging;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;

import java.util.UUID;

/**
 * Interop local filter. Delegates apply to native platform, uses id to identify native target.
 */
public class PlatformMessageLocalFilter implements PlatformMessageFilter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected final long hnd;

    /** */
    protected final PlatformContext platformCtx;

    /**
     * Constructor.
     *
     * @param hnd Handle in the native platform.
     * @param ctx Context.
     */
    public PlatformMessageLocalFilter(long hnd, PlatformContext ctx) {
        assert ctx != null;
        assert hnd != 0;

        this.hnd = hnd;
        this.platformCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(UUID uuid, Object m) {
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = platformCtx.writer(out);

            writer.writeObject(uuid);
            writer.writeObject(m);

            out.synchronize();

            int res = platformCtx.gateway().messagingFilterApply(hnd, mem.pointer());

            return res != 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        platformCtx.gateway().messagingFilterDestroy(hnd);
    }

    /** {@inheritDoc} */
    @Override public void initialize(GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PlatformMessageLocalFilter filter = (PlatformMessageLocalFilter)o;

        return hnd == filter.hnd;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(hnd ^ (hnd >>> 32));
    }
}

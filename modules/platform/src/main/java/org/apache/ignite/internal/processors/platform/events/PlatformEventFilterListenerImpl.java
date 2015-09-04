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

package org.apache.ignite.internal.processors.platform.events;

import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformEventFilterListener;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import java.util.UUID;

/**
 * Platform event filter. Delegates apply to native platform.
 */
public class PlatformEventFilterListenerImpl implements PlatformEventFilterListener
{
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Object pred;

    /** Event types. */
    private final int[] types;

    /** */
    protected transient long hnd;

    /** */
    private transient PlatformContext ctx;

    /**
     * Constructor.
     *
     * @param hnd Handle in the native platform.
     * @param ctx Context.
     */
    public PlatformEventFilterListenerImpl(long hnd, PlatformContext ctx) {
        assert ctx != null;
        assert hnd != 0;

        this.hnd = hnd;
        this.ctx = ctx;

        pred = null;
        types = null;
    }

    /**
     * Constructor.
     *
     * @param pred .Net portable predicate.
     */
    public PlatformEventFilterListenerImpl(Object pred, final int... types) {
        assert pred != null;

        this.pred = pred;
        this.types = types;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Event evt) {
        return apply0(null, evt);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(UUID uuid, Event evt) {
        return apply0(uuid, evt);
    }

    /**
     * Apply impl.
     * @param uuid Node if.
     * @param evt Event.
     * @return Result.
     */
    private boolean apply0(final UUID uuid, final Event evt) {
        if (!ctx.isEventTypeSupported(evt.type()))
            return false;

        if (types != null) {
            boolean match = false;

            for (int type : types) {
                if (type == evt.type()) {
                    match = true;
                    break;
                }
            }

            if (!match)
                return false;
        }

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            ctx.writeEvent(writer, evt);

            writer.writeUuid(uuid);

            out.synchronize();

            int res = ctx.gateway().eventFilterApply(hnd, mem.pointer());

            return res != 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        ctx.gateway().eventFilterDestroy(hnd);
    }

    /** {@inheritDoc} */
    @Override public void initialize(GridKernalContext gridCtx) {
        ctx = PlatformUtils.platformContext(gridCtx.grid());

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writer.writeObjectDetached(pred);

            out.synchronize();

            hnd = ctx.gateway().eventFilterCreate(mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || o != null && o instanceof PlatformEventFilterListenerImpl &&
            hnd == ((PlatformEventFilterListenerImpl)o).hnd;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(hnd ^ (hnd >>> 32));
    }
}
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
import org.apache.ignite.internal.processors.platform.PlatformAbstractPredicate;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import java.util.UUID;

/**
 * Platform message filter. Delegates apply to native platform.
 */
public class PlatformMessageFilterImpl extends PlatformAbstractPredicate implements PlatformMessageFilter {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     */
    public PlatformMessageFilterImpl()
    {
        super();
    }

    /**
     * Constructor.
     *
     * @param pred .Net portable predicate.
     * @param ptr Pointer to predicate in the native platform.
     * @param ctx Kernal context.
     */
    public PlatformMessageFilterImpl(Object pred, long ptr, PlatformContext ctx) {
        super(pred, ptr, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(UUID uuid, Object m) {
        if (ptr == 0)
            return false;  // Destroyed.

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writer.writeObject(uuid);
            writer.writeObject(m);

            out.synchronize();

            return ctx.gateway().messagingFilterApply(ptr, mem.pointer()) != 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(GridKernalContext kernalCtx) {
        if (ptr != 0)
            return;

        ctx = PlatformUtils.platformContext(kernalCtx.grid());

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writer.writeObject(pred);

            out.synchronize();

            ptr = ctx.gateway().messagingFilterCreate(mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        if (ptr == 0) // Already destroyed or not initialized yet.
            return;

        try {
            assert ctx != null;

            ctx.gateway().messagingFilterDestroy(ptr);
        }
        finally {
            ptr = 0;
        }
    }
}
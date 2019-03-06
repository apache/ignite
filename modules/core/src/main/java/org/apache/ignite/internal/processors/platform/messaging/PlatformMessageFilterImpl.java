/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.messaging;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
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
     * @param pred .Net binary predicate.
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

            BinaryRawWriterEx writer = ctx.writer(out);

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

            BinaryRawWriterEx writer = ctx.writer(out);

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

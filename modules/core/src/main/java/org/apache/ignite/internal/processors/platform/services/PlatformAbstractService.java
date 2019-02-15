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

package org.apache.ignite.internal.processors.platform.services;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceContext;

/**
 * Base platform service implementation.
 */
public abstract class PlatformAbstractService implements PlatformService, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** .Net binary service. */
    protected Object svc;

    /** Whether to keep objects binary on server if possible. */
    protected boolean srvKeepBinary;

    /** Pointer to deployed service. */
    protected transient long ptr;

    /** Context. */
    protected transient PlatformContext platformCtx;

    /**
     * Default constructor for serialization.
     */
    public PlatformAbstractService() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param svc Service.
     * @param ctx Context.
     * @param srvKeepBinary Whether to keep objects binary on server if possible.
     */
    public PlatformAbstractService(Object svc, PlatformContext ctx, boolean srvKeepBinary) {
        assert svc != null;
        assert ctx != null;

        this.svc = svc;
        this.platformCtx = ctx;
        this.srvKeepBinary = srvKeepBinary;
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        assert ptr == 0;
        assert platformCtx != null;

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeBoolean(srvKeepBinary);
            writer.writeObject(svc);

            writeServiceContext(ctx, writer);

            out.synchronize();

            ptr = platformCtx.gateway().serviceInit(mem.pointer());

            PlatformInputStream in = mem.input();

            in.synchronize();

            BinaryRawReaderEx reader = platformCtx.reader(in);

            PlatformUtils.readInvocationResult(platformCtx, reader);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        assert ptr != 0;
        assert platformCtx != null;

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeLong(ptr);

            writer.writeBoolean(srvKeepBinary);

            writeServiceContext(ctx, writer);

            out.synchronize();

            platformCtx.gateway().serviceExecute(mem.pointer());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        assert ptr != 0;
        assert platformCtx != null;

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeLong(ptr);

            writer.writeBoolean(srvKeepBinary);

            writeServiceContext(ctx, writer);

            out.synchronize();

            platformCtx.gateway().serviceCancel(mem.pointer());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Writes service context.
     *
     * @param ctx Context.
     * @param writer Writer.
     */
    private void writeServiceContext(ServiceContext ctx, BinaryRawWriterEx writer) {
        writer.writeString(ctx.name());
        writer.writeUuid(ctx.executionId());
        writer.writeBoolean(ctx.isCancelled());
        writer.writeString(ctx.cacheName());
        writer.writeObject(ctx.affinityKey());
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        assert ptr != 0;

        return ptr;
    }

    /** {@inheritDoc} */
    @Override public Object invokeMethod(String mthdName, boolean srvKeepBinary, Object[] args)
        throws IgniteCheckedException {
        assert ptr != 0;
        assert platformCtx != null;

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeLong(ptr);
            writer.writeBoolean(srvKeepBinary);
            writer.writeString(mthdName);

            if (args == null)
                writer.writeBoolean(false);
            else {
                writer.writeBoolean(true);
                writer.writeInt(args.length);

                for (Object arg : args)
                    writer.writeObjectDetached(arg);
            }

            out.synchronize();

            platformCtx.gateway().serviceInvokeMethod(mem.pointer());

            PlatformInputStream in = mem.input();

            in.synchronize();

            BinaryRawReaderEx reader = platformCtx.reader(in);

            return PlatformUtils.readInvocationResult(platformCtx, reader);
        }
    }

    /**
     * @param ignite Ignite instance.
     */
    @IgniteInstanceResource
    public void setIgniteInstance(Ignite ignite) {
        // Ignite instance can be null here because service processor invokes "cleanup" on resource manager.
        if (ignite != null && platformCtx == null)
            platformCtx = PlatformUtils.platformContext(ignite);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        svc = in.readObject();
        srvKeepBinary = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(svc);
        out.writeBoolean(srvKeepBinary);
    }
}

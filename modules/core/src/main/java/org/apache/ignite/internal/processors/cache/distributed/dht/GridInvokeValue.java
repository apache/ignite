/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridInvokeValue implements Message {
    /** */
    private static final long serialVersionUID = 1L;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[] invokeArgsBytes;

    /** Entry processors. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProcessor;

    /** Entry processors bytes. */
    private byte[] entryProcessorBytes;

    /**
     * Constructor.
     */
    public GridInvokeValue() {
    }

    /**
     * Constructor.
     *
     * @param entryProcessor Entry processor.
     * @param invokeArgs Entry processor invoke arguments.
     */
    public GridInvokeValue(EntryProcessor<Object, Object, Object> entryProcessor, Object[] invokeArgs) {
        this.invokeArgs = invokeArgs;
        this.entryProcessor = entryProcessor;
    }

    /**
     * @return Invoke arguments.
     */
    public Object[] invokeArgs() {
        return invokeArgs;
    }

    /**
     * @return Entry processor.
     */
    public EntryProcessor<Object, Object, Object> entryProcessor() {
        return entryProcessor;
    }

    /**
     * Marshalls invoke value.
     *
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        if (entryProcessor != null && entryProcessorBytes == null) {
            entryProcessorBytes = CU.marshal(ctx, entryProcessor);
        }

        if (invokeArgsBytes == null)
            invokeArgsBytes = CU.marshal(ctx, invokeArgs);
    }

    /**
     * Unmarshalls invoke value.
     *
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If un-marshalling failed.
     */
    public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (entryProcessorBytes != null && entryProcessor == null)
            entryProcessor = U.unmarshal(ctx, entryProcessorBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (invokeArgs == null)
            invokeArgs = U.unmarshal(ctx, invokeArgsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("entryProcessorBytes", entryProcessorBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("invokeArgsBytes", invokeArgsBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                entryProcessorBytes = reader.readByteArray("entryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                invokeArgsBytes = reader.readByteArray("invokeArgsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridInvokeValue.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 161;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}

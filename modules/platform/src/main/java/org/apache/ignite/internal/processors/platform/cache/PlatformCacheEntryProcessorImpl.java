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

package org.apache.ignite.internal.processors.platform.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Platform cache entry processor. Delegates processing to native platform.
 */
public class PlatformCacheEntryProcessorImpl implements PlatformCacheEntryProcessor, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Indicates that entry has not been modified  */
    private static final byte ENTRY_STATE_INTACT = 0;

    /** Indicates that entry value has been set  */
    private static final byte ENTRY_STATE_VALUE_SET = 1;

    /** Indicates that remove has been called on an entry  */
    private static final byte ENTRY_STATE_REMOVED = 2;

    /** Indicates error in processor that is written as portable.  */
    private static final byte ENTRY_STATE_ERR_PORTABLE = 3;

    /** Indicates error in processor that is written as string.  */
    private static final byte ENTRY_STATE_ERR_STRING = 4;

    /** Native portable processor */
    private Object proc;

    /** Pointer to processor in the native platform. */
    private transient long ptr;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformCacheEntryProcessorImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param proc Native portable processor
     * @param ptr Pointer to processor in the native platform.
     */
    public PlatformCacheEntryProcessorImpl(Object proc, long ptr) {
        this.proc = proc;
        this.ptr = ptr;
    }

    /** {@inheritDoc} */
    @Override public Object process(MutableEntry entry, Object... args)
        throws EntryProcessorException {
        try {
            IgniteKernal ignite = (IgniteKernal)entry.unwrap(Ignite.class);

            PlatformProcessor interopProc;

            try {
                interopProc = PlatformUtils.platformProcessor(ignite);
            }
            catch (IllegalStateException ex){
                throw new EntryProcessorException(ex);
            }

            interopProc.awaitStart();

            return execute0(interopProc.context(), entry);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Executes interop entry processor on a given entry, updates entry and returns result.
     *
     * @param ctx Context.
     * @param entry Entry.
     * @return Processing result.
     * @throws org.apache.ignite.IgniteCheckedException
     */
    private Object execute0(PlatformContext ctx, MutableEntry entry)
        throws IgniteCheckedException {
        try (PlatformMemory outMem = ctx.memory().allocate()) {
            PlatformOutputStream out = outMem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writeEntryAndProcessor(entry, writer);

            out.synchronize();

            try (PlatformMemory inMem = ctx.memory().allocate()) {
                PlatformInputStream in = inMem.input();

                ctx.gateway().cacheInvoke(outMem.pointer(), inMem.pointer());

                in.synchronize();

                PortableRawReaderEx reader = ctx.reader(in);

                return readResultAndUpdateEntry(ctx, entry, reader);
            }
        }
    }

    /**
     * Writes mutable entry and entry processor to the stream.
     *
     * @param entry Entry to process.
     * @param writer Writer.
     */
    private void writeEntryAndProcessor(MutableEntry entry, PortableRawWriterEx writer) {
        writer.writeObject(entry.getKey());
        writer.writeObject(entry.getValue());

        if (ptr != 0) {
            // Execute locally - we have a pointer to native processor.
            writer.writeBoolean(true);
            writer.writeLong(ptr);
        }
        else {
            // We are on a remote node. Send processor holder back to native.
            writer.writeBoolean(false);
            writer.writeObject(proc);
        }
    }

    /**
     * Reads processing result from stream, updates mutable entry accordingly, and returns the result.
     *
     * @param entry Mutable entry to update.
     * @param reader Reader.
     * @return Entry processing result
     * @throws javax.cache.processor.EntryProcessorException If processing has failed in user code.
     */
    @SuppressWarnings("unchecked")
    private Object readResultAndUpdateEntry(PlatformContext ctx, MutableEntry entry, PortableRawReaderEx reader) {
        byte state = reader.readByte();

        switch (state) {
            case ENTRY_STATE_VALUE_SET:
                entry.setValue(reader.readObjectDetached());

                break;

            case ENTRY_STATE_REMOVED:
                entry.remove();

                break;

            case ENTRY_STATE_ERR_PORTABLE:
                // Full exception
                Object nativeErr = reader.readObjectDetached();

                assert nativeErr != null;

                throw new EntryProcessorException("Failed to execute native cache entry processor.",
                    ctx.createNativeException(nativeErr));

            case ENTRY_STATE_ERR_STRING:
                // Native exception was not serializable, we have only message.
                String errMsg = reader.readString();

                assert errMsg != null;

                throw new EntryProcessorException("Failed to execute native cache entry processor: " + errMsg);

            default:
                assert state == ENTRY_STATE_INTACT;
        }

        return reader.readObjectDetached();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(proc);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        proc = in.readObject();
    }
}
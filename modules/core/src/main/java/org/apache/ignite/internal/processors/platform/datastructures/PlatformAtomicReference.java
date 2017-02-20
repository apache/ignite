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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicReferenceImpl;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;

/**
 * Platform atomic reference wrapper.
 */
@SuppressWarnings("unchecked")
public class PlatformAtomicReference extends PlatformAbstractTarget {
    /** */
    private static final int OP_GET = 1;

    /** */
    private static final int OP_SET = 2;

    /** */
    private static final int OP_COMPARE_AND_SET_AND_GET = 3;

    /** */
    private static final int OP_CLOSE = 4;

    /** */
    private static final int OP_IS_CLOSED = 5;

    /** */
    private final GridCacheAtomicReferenceImpl atomicRef;

    /**
     * Creates an instance or returns null.
     *
     * @param ctx Context.
     * @param name Name.
     * @param memPtr Pointer to a stream with initial value. 0 for default value.
     * @param create Create flag.
     * @return Instance of a PlatformAtomicReference, or null when Ignite reference with specific name is null.
     */
    public static PlatformAtomicReference createInstance(PlatformContext ctx, String name, long memPtr,
        boolean create) {
        assert ctx != null;
        assert name != null;

        Object initVal = null;

        if (memPtr != 0) {
            try (PlatformMemory mem = ctx.memory().get(memPtr)) {
                initVal = ctx.reader(mem).readObjectDetached();
            }
        }

        GridCacheAtomicReferenceImpl atomicRef =
            (GridCacheAtomicReferenceImpl)ctx.kernalContext().grid().atomicReference(name, initVal, create);

        if (atomicRef == null)
            return null;

        return new PlatformAtomicReference(ctx, atomicRef);
    }

    /**
     * Ctor.
     *
     * @param ctx Context.
     * @param ref Atomic reference to wrap.
     */
    private PlatformAtomicReference(PlatformContext ctx, GridCacheAtomicReferenceImpl ref) {
        super(ctx);

        assert ref != null;

        atomicRef = ref;
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_GET)
            writer.writeObject(atomicRef.get());
        else
            super.processOutStream(type, writer);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        if (type == OP_SET) {
            atomicRef.set(reader.readObjectDetached());

            return 0;
        }

        return super.processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader,
        BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_COMPARE_AND_SET_AND_GET) {
            Object val = reader.readObjectDetached();
            final Object cmp = reader.readObjectDetached();

            Object res = atomicRef.compareAndSetAndGet(val, cmp);

            if (cmp == res)
                writer.writeBoolean(true);
            else {
                writer.writeBoolean(false);
                writer.writeObject(res);
            }
        }
        else
            super.processInStreamOutStream(type, reader, writer);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_CLOSE:
                atomicRef.close();

                return TRUE;

            case OP_IS_CLOSED:
                return atomicRef.removed() ? TRUE : FALSE;
        }

        return super.processInLongOutLong(type, val);
    }
}
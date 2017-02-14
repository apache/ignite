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

package org.apache.ignite.internal.binary;

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Binary objects marshaller.
 */
public class GridBinaryMarshaller {
    /** */
    public static final ThreadLocal<Boolean> KEEP_BINARIES = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return true;
        }
    };

    /** Binary context in TLS store. */
    private static final ThreadLocal<BinaryContext> BINARY_CTX = new ThreadLocal<>();

    /** */
    public static final byte OPTM_MARSH = -2;

    /** */
    public static final byte BYTE = 1;

    /** */
    public static final byte SHORT = 2;

    /** */
    public static final byte INT = 3;

    /** */
    public static final byte LONG = 4;

    /** */
    public static final byte FLOAT = 5;

    /** */
    public static final byte DOUBLE = 6;

    /** */
    public static final byte CHAR = 7;

    /** */
    public static final byte BOOLEAN = 8;

    /** */
    public static final byte DECIMAL = 30;

    /** */
    public static final byte STRING = 9;

    /** */
    public static final byte UUID = 10;

    /** */
    public static final byte DATE = 11;

    /** */
    public static final byte BYTE_ARR = 12;

    /** */
    public static final byte SHORT_ARR = 13;

    /** */
    public static final byte INT_ARR = 14;

    /** */
    public static final byte LONG_ARR = 15;

    /** */
    public static final byte FLOAT_ARR = 16;

    /** */
    public static final byte DOUBLE_ARR = 17;

    /** */
    public static final byte CHAR_ARR = 18;

    /** */
    public static final byte BOOLEAN_ARR = 19;

    /** */
    public static final byte DECIMAL_ARR = 31;

    /** */
    public static final byte STRING_ARR = 20;

    /** */
    public static final byte UUID_ARR = 21;

    /** */
    public static final byte DATE_ARR = 22;

    /** */
    public static final byte OBJ_ARR = 23;

    /** */
    public static final byte COL = 24;

    /** */
    public static final byte MAP = 25;

    /** */
    public static final byte BINARY_OBJ = 27;

    /** */
    public static final byte ENUM = 28;

    /** */
    public static final byte ENUM_ARR = 29;

    /** */
    public static final byte CLASS = 32;

    /** Timestamp. */
    public static final byte TIMESTAMP = 33;

    /** Timestamp array. */
    public static final byte TIMESTAMP_ARR = 34;

    /** Proxy. */
    public static final byte PROXY = 35;

    /** Time. */
    public static final byte TIME = 36;

    /** Time array. */
    public static final byte TIME_ARR = 37;

    /** */
    public static final byte NULL = (byte)101;

    /** */
    public static final byte HANDLE = (byte)102;

    /** */
    public static final byte OBJ = (byte)103;

    /** */
    public static final byte USER_SET = -1;

    /** */
    public static final byte USER_COL = 0;

    /** */
    public static final byte ARR_LIST = 1;

    /** */
    public static final byte LINKED_LIST = 2;

    /** */
    public static final byte HASH_SET = 3;

    /** */
    public static final byte LINKED_HASH_SET = 4;

    /** */
    public static final byte HASH_MAP = 1;

    /** */
    public static final byte LINKED_HASH_MAP = 2;

    /** */
    public static final byte PLATFORM_JAVA_OBJECT_FACTORY_PROXY = 99;

    /** */
    public static final int OBJECT = -1;

    /** */
    public static final int UNREGISTERED_TYPE_ID = 0;

    /** Protocol version. */
    public static final byte PROTO_VER = 1;

    /** Protocol version position. */
    public static final int PROTO_VER_POS = 1;

    /** Flags position in header. */
    public static final int FLAGS_POS = 2;

    /** */
    public static final int TYPE_ID_POS = 4;

    /** */
    public static final int HASH_CODE_POS = 8;

    /** */
    public static final int TOTAL_LEN_POS = 12;

    /** */
    public static final int SCHEMA_ID_POS = 16;

    /** Schema or raw offset position. */
    public static final int SCHEMA_OR_RAW_OFF_POS = 20;

    /** */
    public static final byte DFLT_HDR_LEN = 24;

    /** */
    private final BinaryContext ctx;

    /**
     * @param ctx Context.
     */
    public GridBinaryMarshaller(BinaryContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public byte[] marshal(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            return new byte[] { NULL };

        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx)) {
            writer.marshal(obj);

            return writer.array();
        }
    }

    /**
     * @param bytes Bytes array.
     * @return Binary object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T unmarshal(byte[] bytes, @Nullable ClassLoader clsLdr) throws BinaryObjectException {
        assert bytes != null;

        BinaryContext oldCtx = pushContext(ctx);

        try {
            return (T) BinaryUtils.unmarshal(BinaryHeapInputStream.create(bytes, 0), ctx, clsLdr);
        }
        finally {
            popContext(oldCtx);
        }
    }

    /**
     * @param in Input stream.
     * @return Binary object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T unmarshal(BinaryInputStream in) throws BinaryObjectException {
        BinaryContext oldCtx = pushContext(ctx);

        try {
            return (T)BinaryUtils.unmarshal(in, ctx, null);
        }
        finally {
            popContext(oldCtx);
        }
    }

    /**
     * @param arr Byte array.
     * @param ldr Class loader.
     * @return Deserialized object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T deserialize(byte[] arr, @Nullable ClassLoader ldr) throws BinaryObjectException {
        assert arr != null;
        assert arr.length > 0;

        if (arr[0] == NULL)
            return null;

        BinaryContext oldCtx = pushContext(ctx);

        try {
            return (T)new BinaryReaderExImpl(ctx, BinaryHeapInputStream.create(arr, 0), ldr, true).deserialize();
        }
        finally {
            popContext(oldCtx);
        }
    }

    /**
     * Push binary context and return the old one.
     *
     * @return Old binary context.
     */
    public BinaryContext pushContext() {
        return pushContext(ctx);
    }

    /**
     * Push binary context and return the old one.
     *
     * @param ctx Binary context.
     * @return Old binary context.
     */
    @Nullable private static BinaryContext pushContext(BinaryContext ctx) {
        BinaryContext old = BINARY_CTX.get();

        BINARY_CTX.set(ctx);

        return old;
    }

    /**
     * Pop binary context and restore the old one.
     *
     * @param oldCtx Old binary context.
     */
    public static void popContext(@Nullable BinaryContext oldCtx) {
        BINARY_CTX.set(oldCtx);
    }

    /**
     * Creates a reader.
     *
     * @param stream Stream.
     * @return Reader.
     */
    public BinaryReaderExImpl reader(BinaryInputStream stream) {
        assert stream != null;

        return new BinaryReaderExImpl(ctx, stream, null, true);
    }

    /**
     * Whether object must be deserialized anyway. I.e. it cannot be converted to BinaryObject.
     *
     * @param obj Object.
     * @return {@code True} if object will be deserialized on unmarshal.
     */
    public boolean mustDeserialize(Object obj) {
        return obj != null && ctx.mustDeserialize(obj.getClass());
    }

    /**
     * Gets writer for the given output stream.
     *
     * @param out Output stream.
     * @return Writer.
     */
    public BinaryWriterExImpl writer(BinaryOutputStream out) {
        return new BinaryWriterExImpl(ctx, out, BinaryThreadLocalContext.get().schemaHolder(), null);
    }

    /**
     * @return Context.
     */
    public BinaryContext context() {
        return ctx;
    }

    /**
     * @return Thread-bound context.
     */
    public static BinaryContext threadLocalContext() {
        BinaryContext ctx = GridBinaryMarshaller.BINARY_CTX.get();

        if (ctx == null) {
            IgniteKernal ignite = IgnitionEx.localIgnite();

            IgniteCacheObjectProcessor proc = ignite.context().cacheObjects();

            if (proc instanceof CacheObjectBinaryProcessorImpl)
                return ((CacheObjectBinaryProcessorImpl)proc).binaryContext();
            else
                throw new IgniteIllegalStateException("Ignite instance must be started with " +
                    BinaryMarshaller.class.getName() + " [name=" + ignite.name() + ']');
        }

        return ctx;
    }
}

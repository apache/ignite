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

import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.binary.BinaryObjectException;
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

    /** */
    static final byte OPTM_MARSH = -2;

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
    public static final int OBJECT_TYPE_ID = -1;

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

        return (T)BinaryUtils.unmarshal(BinaryHeapInputStream.create(bytes, 0), ctx, clsLdr);
    }

    /**
     * @param in Input stream.
     * @return Binary object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T unmarshal(BinaryInputStream in) throws BinaryObjectException {
        return (T)BinaryUtils.unmarshal(in, ctx, null);
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

        return (T)new BinaryReaderExImpl(ctx, BinaryHeapInputStream.create(arr, 0), ldr).deserialize();
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
}

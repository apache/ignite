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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.portable.streams.*;
import org.apache.ignite.portable.*;

import org.jetbrains.annotations.*;

/**
 * Portable objects marshaller.
 */
public class GridPortableMarshaller {
    /** */
    public static final ThreadLocal<Boolean> KEEP_PORTABLES = new ThreadLocal<Boolean>() {
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
    public static final byte MAP_ENTRY = 26;

    /** */
    public static final byte PORTABLE_OBJ = 27;

    /** */
    public static final byte ENUM = 28;

    /** */
    public static final byte ENUM_ARR = 29;

    /** */
    public static final byte CLASS = 32;

    /** */
    public static final byte NULL = (byte)101;

    /** */
    public static final byte HANDLE = (byte)102;

    /** */
    public static final byte OBJ = (byte)103;

    /** */
    static final byte USER_SET = -1;

    /** */
    static final byte USER_COL = 0;

    /** */
    static final byte ARR_LIST = 1;

    /** */
    static final byte LINKED_LIST = 2;

    /** */
    static final byte HASH_SET = 3;

    /** */
    static final byte LINKED_HASH_SET = 4;

    /** */
    static final byte TREE_SET = 5;

    /** */
    static final byte CONC_SKIP_LIST_SET = 6;

    /** */
    static final byte HASH_MAP = 1;

    /** */
    static final byte LINKED_HASH_MAP = 2;

    /** */
    static final byte TREE_MAP = 3;

    /** */
    static final byte CONC_HASH_MAP = 4;

    /** */
    static final byte PROPERTIES_MAP = 5;

    /** */
    static final int OBJECT_TYPE_ID = -1;

    /** */
    static final int UNREGISTERED_TYPE_ID = 0;

    /** */
    static final int TYPE_ID_POS = 2;

    /** */
    static final int HASH_CODE_POS = 6;

    /** */
    static final int TOTAL_LEN_POS = 10;

    /** */
    static final byte RAW_DATA_OFF_POS = 14;

    /** */
    static final int CLS_NAME_POS = 18;

    /** */
    static final byte DFLT_HDR_LEN = 18;

    /** */
    private final PortableContext ctx;

    /**
     * @param ctx Context.
     */
    public GridPortableMarshaller(PortableContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param obj Object to marshal.
     * @param off Offset.
     * @return Byte array.
     * @throws PortableException In case of error.
     */
    public byte[] marshal(@Nullable Object obj, int off) throws PortableException {
        if (obj == null)
            return new byte[] { NULL };

        try (PortableWriterExImpl writer = new PortableWriterExImpl(ctx, off)) {
            writer.marshal(obj, false);

            return writer.array();
        }
    }

    /**
     * @param bytes Bytes array.
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T unmarshal(byte[] bytes, @Nullable ClassLoader clsLdr) throws PortableException {
        assert bytes != null;

        PortableReaderExImpl reader = new PortableReaderExImpl(ctx, bytes, 0, clsLdr);

        return (T)reader.unmarshal();
    }

    /**
     * @param in Input stream.
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T unmarshal(PortableInputStream in) throws PortableException {
        return (T)reader(in).unmarshal();
    }

    /**
     * @param arr Byte array.
     * @param ldr Class loader.
     * @return Deserialized object.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T deserialize(byte[] arr, @Nullable ClassLoader ldr) throws PortableException {
        assert arr != null;
        assert arr.length > 0;

        if (arr[0] == NULL)
            return null;

        PortableReaderExImpl reader = new PortableReaderExImpl(ctx, arr, 0, ldr);

        return (T)reader.deserialize();
    }

    /**
     * Gets writer for the given output stream.
     *
     * @param out Output stream.
     * @return Writer.
     */
    public PortableWriterExImpl writer(PortableOutputStream out) {
        return new PortableWriterExImpl(ctx, out, 0);
    }

    /**
     * Gets reader for the given input stream.
     *
     * @param in Input stream.
     * @return Reader.
     */
    public PortableReaderExImpl reader(PortableInputStream in) {
        // TODO: IGNITE-1272 - Is class loader needed here?
        return new PortableReaderExImpl(ctx, in, in.position(), null);
    }

    /**
     * @return Context.
     */
    public PortableContext context() {
        return ctx;
    }
}

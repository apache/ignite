/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable objects marshaller.
 */
public class GridPortableMarshaller {
    /** */
    public static final byte NULL = (byte)0x00;

    /** */
    public static final byte HANDLE = (byte)0x01;

    /** */
    public static final byte OBJ = (byte)0x02;

    /** */
    public static final byte BYTE = (byte)0x10;

    /** */
    public static final byte SHORT = (byte)0x11;

    /** */
    public static final byte INT = (byte)0x12;

    /** */
    public static final byte LONG = (byte)0x13;

    /** */
    public static final byte FLOAT = (byte)0x14;

    /** */
    public static final byte DOUBLE = (byte)0x15;

    /** */
    public static final byte CHAR = (byte)0x16;

    /** */
    public static final byte BOOLEAN = (byte)0x17;

    /** */
    public static final byte STRING = (byte)0x18;

    /** */
    public static final byte UUID = (byte)0x19;

    /** */
    public static final byte BYTE_ARR = (byte)0x20;

    /** */
    public static final byte SHORT_ARR = (byte)0x21;

    /** */
    public static final byte INT_ARR = (byte)0x22;

    /** */
    public static final byte LONG_ARR = (byte)0x23;

    /** */
    public static final byte FLOAT_ARR = (byte)0x24;

    /** */
    public static final byte DOUBLE_ARR = (byte)0x25;

    /** */
    public static final byte CHAR_ARR = (byte)0x26;

    /** */
    public static final byte BOOLEAN_ARR = (byte)0x27;

    /** */
    public static final byte STRING_ARR = (byte)0x28;

    /** */
    public static final byte UUID_ARR = (byte)0x29;

    /** */
    public static final byte OBJ_ARR = (byte)0x30;

    /** */
    public static final byte COL = (byte)0x31;

    /** */
    public static final byte MAP = (byte)0x32;

    /** */
    private static final ByteBuffer NULL_BUF = ByteBuffer.wrap(new byte[] { NULL });

    /** */
    private final GridPortableContext ctx;

    /**
     * @param ctx Context.
     */
    public GridPortableMarshaller(GridPortableContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param obj Object to marshal.
     * @return Byte buffer.
     * @throws GridPortableException In case of error.
     */
    public ByteBuffer marshal(@Nullable Object obj) throws GridPortableException {
        if (obj == null)
            return NULL_BUF;

        GridPortableWriterImpl writer = new GridPortableWriterImpl(ctx);

        writer.marshal(obj);

        return writer.buffer();
    }

    /**
     * @param arr Byte array.
     * @return Portable object.
     * @throws GridPortableException
     */
    @Nullable public GridPortableObject unmarshal(byte[] arr) throws GridPortableException {
        assert arr != null;
        assert arr.length > 0;

        if (arr.length == 1 && arr[0] == NULL)
            return null;

        GridPortableReaderImpl reader = new GridPortableReaderImpl(ctx, arr);

        return (GridPortableObject)reader.unmarshal(0);
    }
}

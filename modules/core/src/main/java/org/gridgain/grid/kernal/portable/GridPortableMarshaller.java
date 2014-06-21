/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable objects marshaller.
 */
public class GridPortableMarshaller {
    /** */
    static final byte NULL = (byte)0x00;

    /** */
    static final byte HANDLE = (byte)0x01;

    /** */
    static final byte OBJ = (byte)0x02;

    /** */
    static final byte BYTE = (byte)0x10;

    /** */
    static final byte SHORT = (byte)0x11;

    /** */
    static final byte INT = (byte)0x12;

    /** */
    static final byte LONG = (byte)0x13;

    /** */
    static final byte FLOAT = (byte)0x14;

    /** */
    static final byte DOUBLE = (byte)0x15;

    /** */
    static final byte CHAR = (byte)0x16;

    /** */
    static final byte BOOLEAN = (byte)0x17;

    /** */
    static final byte STRING = (byte)0x18;

    /** */
    static final byte UUID = (byte)0x19;

    /** */
    static final byte BYTE_ARR = (byte)0x20;

    /** */
    static final byte SHORT_ARR = (byte)0x21;

    /** */
    static final byte INT_ARR = (byte)0x22;

    /** */
    static final byte LONG_ARR = (byte)0x23;

    /** */
    static final byte FLOAT_ARR = (byte)0x24;

    /** */
    static final byte DOUBLE_ARR = (byte)0x25;

    /** */
    static final byte CHAR_ARR = (byte)0x26;

    /** */
    static final byte BOOLEAN_ARR = (byte)0x27;

    /** */
    static final byte STRING_ARR = (byte)0x28;

    /** */
    static final byte UUID_ARR = (byte)0x29;

    /** */
    static final byte OBJ_ARR = (byte)0x30;

    /** */
    static final byte COL = (byte)0x31;

    /** */
    static final byte MAP = (byte)0x32;

    /** */
    private static final ByteBuffer NULL_BUF = ByteBuffer.wrap(new byte[] { NULL });

    /**
     * @param obj Object to marshal.
     * @return Byte buffer.
     * @throws GridPortableException In case of error.
     */
    public ByteBuffer marshal(@Nullable Object obj) throws GridPortableException {
        if (obj == null)
            return NULL_BUF;

        GridPortableWriterImpl writer = new GridPortableWriterImpl();

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

        GridPortableReaderImpl reader = new GridPortableReaderImpl(arr);

        return (GridPortableObject)reader.unmarshal(0);
    }
}

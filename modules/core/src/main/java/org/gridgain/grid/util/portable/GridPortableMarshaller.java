/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable objects marshaller.
 */
public class GridPortableMarshaller {
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
    static final byte NULL = (byte)101;

    /** */
    static final byte HANDLE = (byte)102;

    /** */
    static final byte OBJ = (byte)103;

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
    public ByteBuffer marshal(@Nullable Object obj, int off) throws GridPortableException {
        if (obj == null)
            return NULL_BUF;

        GridPortableWriterImpl writer = new GridPortableWriterImpl(ctx, off);

        writer.marshal(obj);

        return writer.buffer();
    }

    /**
     * @param arr Byte array.
     * @return Portable object.
     * @throws GridPortableException
     */
    @Nullable public <T> T unmarshal(byte[] arr) throws GridPortableException {
        assert arr != null;
        assert arr.length > 0;

        if (arr[0] == NULL)
            return null;

        GridPortableRawReader reader = new GridPortableReaderImpl(ctx, arr, 0);

        return (T)reader.readObject();
    }
}

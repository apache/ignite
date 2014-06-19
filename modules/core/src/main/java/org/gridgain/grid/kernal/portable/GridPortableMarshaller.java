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
    static final byte NULL = (byte)0x80;

    /** */
    static final byte HANDLE = (byte)0x81;

    /** */
    static final byte OBJ = (byte)0x82;

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

        writer.doWriteObject(obj);

        return writer.buffer();
    }

    /**
     * @param arr Byte array.
     * @return Portable object.
     * @throws GridPortableException
     */
    @Nullable public <T> T unmarshal(byte[] arr) throws GridPortableException {
        return null;
    }
}

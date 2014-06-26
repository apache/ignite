/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller;

import java.io.*;
import java.nio.*;

/**
 * Marshaller for binary protocol messages.
 */
public interface GridClientMarshaller {
    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal.
     * @param off Start offset.
     * @return Byte buffer.
     * @throws IOException If marshalling failed.
     */
    public ByteBuffer marshal(Object obj, int off) throws IOException;

    /**
     * Unmarshals object from byte array.
     *
     * @param bytes Byte array.
     * @return Unmarshalled object.
     * @throws IOException If unmarshalling failed.
     */
    public <T> T unmarshal(byte[] bytes) throws IOException;
}

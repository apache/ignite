/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.jdk;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Simple marshaller that utilize JDK serialization features.
 */
public class GridClientJdkMarshaller implements GridClientMarshaller {
    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        ByteArrayOutputStream tmp = new ByteArrayOutputStream();

        ObjectOutputStream out = new ObjectOutputStream(tmp);

        out.writeObject(obj);

        out.flush();

        return tmp.toByteArray();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        ByteArrayInputStream tmp = new ByteArrayInputStream(bytes);

        ObjectInputStream in = new ObjectInputStream(tmp);

        try {
            return (T)in.readObject();
        }
        catch (ClassNotFoundException e) {
            throw new IOException("Failed to unmarshal target object: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return U.JDK_CLIENT_PROTO_ID;
    }
}

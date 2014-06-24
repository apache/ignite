/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.portable.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Client marshaller supporting {@link GridPortable}.
 */
public class GridClientPortableMarshaller implements GridClientMarshaller {
    /** */
    private final GridPortableMarshaller marsh = new GridPortableMarshaller(null); // TODO: Context

    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(Object obj, int off) throws IOException {
        try {
            return marsh.marshal(obj, off);
        }
        catch (GridPortableException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        try {
            GridPortableObject po = marsh.unmarshal(bytes);

            return po.deserialize();
        }
        catch (GridPortableException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return U.PORTABLE_OBJECT_PROTO_ID;
    }
}

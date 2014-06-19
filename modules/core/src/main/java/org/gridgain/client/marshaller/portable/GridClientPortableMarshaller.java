/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.portable.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Client marshaller supporting {@link GridPortable}.
 */
public class GridClientPortableMarshaller implements GridClientMarshaller {
    /** */
    private final GridPortableMarshaller marsh;

    /**
     * @param typesMap Map associating portable type identifiers with java classes..
     */
    public GridClientPortableMarshaller(@Nullable Map<Integer, Class<? extends GridPortable>> typesMap) {
        Map<Integer, Class<? extends GridPortable>> types = new HashMap<>();

        if (typesMap != null)
            types.putAll(typesMap);

        types.put(GridClientAuthenticationRequest.PORTABLE_TYPE_ID, GridClientAuthenticationRequest.class);
        types.put(GridClientCacheRequest.PORTABLE_TYPE_ID, GridClientCacheRequest.class);
        types.put(GridClientLogRequest.PORTABLE_TYPE_ID, GridClientLogRequest.class);
        types.put(GridClientNodeBean.PORTABLE_TYPE_ID, GridClientNodeBean.class);
        types.put(GridClientNodeMetricsBean.PORTABLE_TYPE_ID, GridClientNodeMetricsBean.class);
        types.put(GridClientResponse.PORTABLE_TYPE_ID, GridClientResponse.class);
        types.put(GridClientTaskRequest.PORTABLE_TYPE_ID, GridClientTaskRequest.class);
        types.put(GridClientTaskResultBean.PORTABLE_TYPE_ID, GridClientTaskResultBean.class);
        types.put(GridClientTopologyRequest.PORTABLE_TYPE_ID, GridClientTopologyRequest.class);

        marsh = null;//new GridPortableMarshaller(types);
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        try {
            return marsh.marshal(obj);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        try {
            return marsh.unmarshal(bytes);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return U.PORTABLE_OBJECT_PROTO_ID;
    }
}

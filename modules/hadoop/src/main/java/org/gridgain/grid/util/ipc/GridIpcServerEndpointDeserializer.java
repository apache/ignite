/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;

import java.util.*;

/**
 * Grid GridIpcServerEndpoint configuration deserializer.
 */
public class GridIpcServerEndpointDeserializer {
    /**
     * Deserializes IPC server endpoint config into concrete
     * instance of {@link GridIpcServerEndpoint}.
     *
     * @param endpointCfg Map with properties of the IPC server endpoint config.
     * @return Deserialized instance of {@link GridIpcServerEndpoint}.
     * @throws GridException If any problem with configuration properties setting has happened.
     */
    public static GridIpcServerEndpoint deserialize(Map<String,String> endpointCfg) throws GridException {
        A.notNull(endpointCfg, "endpointCfg");

        String endpointType = endpointCfg.get("type");

        if (endpointType == null)
            throw new GridException("Failed to create server endpoint (type is not specified)");

        switch (endpointType) {
            case "shmem": {
                GridIpcSharedMemoryServerEndpoint endpoint = new GridIpcSharedMemoryServerEndpoint();

                endpoint.setupConfiguration(endpointCfg);

                return endpoint;
            }
            case "tcp": {
                GridIpcServerTcpEndpoint endpoint = new GridIpcServerTcpEndpoint();

                endpoint.setupConfiguration(endpointCfg);

                return endpoint;
            }
            default:
                throw new GridException("Failed to create server endpoint (type is unknown): " + endpointType);
        }
    }
}

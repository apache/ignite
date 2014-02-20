// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc;

import net.sf.json.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;

/**
 * Grid GridIpcServerEndpoint configuration JSON deserializer.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridIpcServerEndpointDeserializer {
    /**
     * Deserializes JSON-formatted IPC server endpoint config into concrete
     * instance of {@link GridIpcServerEndpoint}.
     *
     * @param endpointCfg JSON-formatted IPC server endpoint config.
     * @return Deserialized instance of {@link GridIpcServerEndpoint}.
     * @throws GridException If any problem with JSON parsing has happened.
     */
    public static GridIpcServerEndpoint deserialize(String endpointCfg) throws GridException {
        A.notNull(endpointCfg, "endpointCfg");

        try {
            JSONObject jsonObj = JSONObject.fromObject(endpointCfg);

            String endpointType = jsonObj.getString("type");

            Class endpointCls;

            switch (endpointType) {
                case "shmem":
                    endpointCls = GridIpcSharedMemoryServerEndpoint.class; break;
                case "tcp":
                    endpointCls = GridIpcServerTcpEndpoint.class; break;
                default:
                    throw new GridException("Failed to create server endpoint (type is unknown): " + endpointType);
            }

            // Remove 'type' entry cause there should not be such field in GridIpcServerEndpoint implementations.
            jsonObj.discard("type");

            return (GridIpcServerEndpoint)JSONObject.toBean(jsonObj, endpointCls);
        }
        catch (JSONException e) {
            throw new GridException("Failed to parse server endpoint.", e);
        }
    }
}

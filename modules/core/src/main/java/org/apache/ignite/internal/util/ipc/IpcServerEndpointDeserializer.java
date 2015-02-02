/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.ipc;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.ipc.loopback.*;
import org.apache.ignite.internal.util.ipc.shmem.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Grid IpcServerEndpoint configuration deserializer.
 */
public class IpcServerEndpointDeserializer {
    /**
     * Deserializes IPC server endpoint config into concrete
     * instance of {@link IpcServerEndpoint}.
     *
     * @param endpointCfg Map with properties of the IPC server endpoint config.
     * @return Deserialized instance of {@link IpcServerEndpoint}.
     * @throws IgniteCheckedException If any problem with configuration properties setting has happened.
     */
    public static IpcServerEndpoint deserialize(Map<String,String> endpointCfg) throws IgniteCheckedException {
        A.notNull(endpointCfg, "endpointCfg");

        String endpointType = endpointCfg.get("type");

        if (endpointType == null)
            throw new IgniteCheckedException("Failed to create server endpoint (type is not specified)");

        switch (endpointType) {
            case "shmem": {
                IpcSharedMemoryServerEndpoint endpoint = new IpcSharedMemoryServerEndpoint();

                endpoint.setupConfiguration(endpointCfg);

                return endpoint;
            }
            case "tcp": {
                IpcServerTcpEndpoint endpoint = new IpcServerTcpEndpoint();

                endpoint.setupConfiguration(endpointCfg);

                return endpoint;
            }
            default:
                throw new IgniteCheckedException("Failed to create server endpoint (type is unknown): " + endpointType);
        }
    }
}

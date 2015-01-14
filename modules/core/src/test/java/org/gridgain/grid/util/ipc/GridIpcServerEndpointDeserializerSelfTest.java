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

package org.gridgain.grid.util.ipc;

import net.sf.json.*;
import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.testframework.*;

import java.util.concurrent.*;

/**
 * Tests for {@code GridIpcServerEndpointDeserializer}.
 */
public class GridIpcServerEndpointDeserializerSelfTest extends GridGgfsCommonAbstractTest {
    /** */
    private GridIpcSharedMemoryServerEndpoint shmemSrvEndpoint;

    /** */
    private GridIpcServerTcpEndpoint tcpSrvEndpoint;

    /**
     * Initialize test stuff.
     */
    @Override protected void beforeTest() throws Exception {
        shmemSrvEndpoint = new GridIpcSharedMemoryServerEndpoint();
        shmemSrvEndpoint.setPort(888);
        shmemSrvEndpoint.setSize(111);
        shmemSrvEndpoint.setTokenDirectoryPath("test-my-path-baby");

        tcpSrvEndpoint = new GridIpcServerTcpEndpoint();
        tcpSrvEndpoint.setPort(999);
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfJsonIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @SuppressWarnings("NullableProblems")
            @Override public Object call() throws Exception {
                return GridIpcServerEndpointDeserializer.deserialize(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: endpointCfg");
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfShmemAndNoTypeInfoInJson() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(
                    JSONSerializer.toJSON(shmemSrvEndpoint).toString()));
            }
        }, IgniteCheckedException.class, "Failed to create server endpoint (type is not specified)");
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfShmemAndNoUnknownTypeInfoInJson() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                JSONObject json = (JSONObject)JSONSerializer.toJSON(shmemSrvEndpoint);
                json.accumulate("type", "unknownEndpointType");

                return GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(json.toString()));
            }
        }, IgniteCheckedException.class, "Failed to create server endpoint (type is unknown): unknownEndpointType");
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfLoopbackAndJsonIsLightlyBroken() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(
                    JSONSerializer.toJSON(tcpSrvEndpoint).toString()));
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfShmemAndJsonIsOk() throws Exception {
        String json = JSONSerializer.toJSON(shmemSrvEndpoint).toString();

        // Add endpoint type info into json.
        json = "{\"type\" : \"shmem\"," + json.substring(1);

        GridIpcServerEndpoint deserialized = GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(json));

        assertTrue(deserialized instanceof GridIpcSharedMemoryServerEndpoint);

        GridIpcSharedMemoryServerEndpoint deserializedShmemEndpoint = (GridIpcSharedMemoryServerEndpoint)deserialized;

        assertEquals(shmemSrvEndpoint.getPort(), deserializedShmemEndpoint.getPort());
        assertEquals(shmemSrvEndpoint.getSize(), deserializedShmemEndpoint.getSize());
        assertEquals(shmemSrvEndpoint.getTokenDirectoryPath(), deserializedShmemEndpoint.getTokenDirectoryPath());
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfShmemAndJsonIsOkAndDefaultValuesAreSetToFields() throws Exception {
        shmemSrvEndpoint = new GridIpcSharedMemoryServerEndpoint();
        shmemSrvEndpoint.setPort(8);

        String json = JSONSerializer.toJSON(shmemSrvEndpoint).toString();

        // Add endpoint type info into json.
        json = "{\"type\" : \"shmem\"," + json.substring(1);

        GridIpcServerEndpoint deserialized = GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(json));

        assertTrue(deserialized instanceof GridIpcSharedMemoryServerEndpoint);

        GridIpcSharedMemoryServerEndpoint deserializedShmemEndpoint = (GridIpcSharedMemoryServerEndpoint)deserialized;

        assertEquals(shmemSrvEndpoint.getPort(), deserializedShmemEndpoint.getPort());
        assertEquals(shmemSrvEndpoint.getSize(), deserializedShmemEndpoint.getSize());
        assertEquals(shmemSrvEndpoint.getTokenDirectoryPath(), deserializedShmemEndpoint.getTokenDirectoryPath());
    }

    /**
     * @throws Exception In case of any exception.
     */
    public void testDeserializeIfLoopbackAndJsonIsOk() throws Exception {
        String json = JSONSerializer.toJSON(tcpSrvEndpoint).toString();

        // Add endpoint type info into json.
        json = "{\"type\" : \"tcp\"," + json.substring(1);

        GridIpcServerEndpoint deserialized = GridIpcServerEndpointDeserializer.deserialize(GridGgfsTestUtils.jsonToMap(json));

        assertTrue(deserialized instanceof GridIpcServerTcpEndpoint);

        assertEquals(tcpSrvEndpoint.getPort(), deserialized.getPort());
    }
}

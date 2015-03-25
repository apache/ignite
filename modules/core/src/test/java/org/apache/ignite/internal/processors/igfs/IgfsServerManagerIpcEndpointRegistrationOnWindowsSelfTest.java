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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.util.ipc.loopback.*;
import org.apache.ignite.internal.util.ipc.shmem.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;

/**
 * Tests for {@link IgfsServerManager} that checks shmem IPC endpoint registration
 * forbidden for Windows.
 */
public class IgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest
    extends IgfsServerManagerIpcEndpointRegistrationAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testShmemEndpointsRegistration() throws Exception {
        Throwable e = GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteConfiguration cfg = gridConfiguration();

                cfg.setFileSystemConfiguration(igfsConfiguration(IgfsIpcEndpointType.SHMEM,
                    IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, null));

                return G.start(cfg);
            }
        }, IgniteException.class, null);

        assert e.getCause().getCause().getMessage().contains(" should not be configured on Windows (configure " +
            IpcServerTcpEndpoint.class.getSimpleName() + ")");
    }
}

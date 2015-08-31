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

package org.apache.ignite.igfs;

import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * IGFS Hadoop file system IPC loopback self test.
 */
public abstract class IgniteHadoopFileSystemLoopbackAbstractSelfTest extends
    IgniteHadoopFileSystemAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     * @param skipEmbed Skip embedded mode flag.
     */
    protected IgniteHadoopFileSystemLoopbackAbstractSelfTest(IgfsMode mode, boolean skipEmbed) {
        super(mode, skipEmbed, true);
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String gridName) {
        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(DFLT_IPC_PORT + getTestGridIndex(gridName));

        return endpointCfg;
    }
}
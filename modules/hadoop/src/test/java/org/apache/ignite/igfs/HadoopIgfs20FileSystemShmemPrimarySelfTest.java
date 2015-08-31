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

import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * Tests Hadoop 2.x file system in primary mode.
 */
public class HadoopIgfs20FileSystemShmemPrimarySelfTest extends HadoopIgfs20FileSystemAbstractSelfTest {
    /**
     * Creates test in primary mode.
     */
    public HadoopIgfs20FileSystemShmemPrimarySelfTest() {
        super(PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected String primaryFileSystemUriPath() {
        return "igfs://igfs:" + getTestGridName(0) + "@/";
    }

    /** {@inheritDoc} */
    @Override protected String primaryFileSystemConfigPath() {
        return "/modules/core/src/test/config/hadoop/core-site.xml";
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String gridName) {
        IgfsIpcEndpointConfiguration cfg = new IgfsIpcEndpointConfiguration();

        cfg.setType(IgfsIpcEndpointType.SHMEM);
        cfg.setPort(DFLT_IPC_PORT + getTestGridIndex(gridName));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected String secondaryFileSystemUriPath() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected String secondaryFileSystemConfigPath() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration secondaryIpcEndpointConfiguration() {
        assert false;

        return null;
    }
}
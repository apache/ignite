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

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * IGFS Hadoop file system IPC self test.
 */
public abstract class IgniteHadoopFileSystemShmemAbstractSelfTest extends IgniteHadoopFileSystemAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     * @param skipEmbed Skip embedded mode flag.
     */
    protected IgniteHadoopFileSystemShmemAbstractSelfTest(IgfsMode mode, boolean skipEmbed) {
        super(mode, skipEmbed, false);
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String gridName) {
        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.SHMEM);
        endpointCfg.setPort(DFLT_IPC_PORT + getTestGridIndex(gridName));

        return endpointCfg;
    }

    /**
     * Checks correct behaviour in case when we run out of system
     * resources.
     *
     * @throws Exception If error occurred.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testOutOfResources() throws Exception {
        final Collection<IpcEndpoint> eps = new LinkedList<>();

        try {
            IgniteCheckedException e = (IgniteCheckedException)GridTestUtils.assertThrows(log, new Callable<Object>() {
                @SuppressWarnings("InfiniteLoopStatement")
                @Override public Object call() throws Exception {
                    while (true) {
                        IpcEndpoint ep = IpcEndpointFactory.connectEndpoint("shmem:10500", log);

                        eps.add(ep);
                    }
                }
            }, IgniteCheckedException.class, null);

            assertNotNull(e);

            String msg = e.getMessage();

            assertTrue("Invalid exception: " + X.getFullStackTrace(e),
                msg.contains("(error code: 28)") ||
                msg.contains("(error code: 24)") ||
                msg.contains("(error code: 12)"));
        }
        finally {
            for (IpcEndpoint ep : eps)
                ep.close();
        }
    }
}
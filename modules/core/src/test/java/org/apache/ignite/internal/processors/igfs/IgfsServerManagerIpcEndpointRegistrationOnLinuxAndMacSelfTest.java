/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

/**
 * Tests for {@link IgfsServer} that checks all IPC endpoint registration types
 * permitted for Linux and Mac OS.
 */
public class IgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest
    extends IgfsServerManagerIpcEndpointRegistrationAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoopbackAndShmemEndpointsRegistration() throws Exception {
        IgniteConfiguration cfg = gridConfigurationManyIgfsCaches(3);

        cfg.setFileSystemConfiguration(
            // Check null IPC endpoint config won't bring any hassles.
            igfsConfiguration(null, null, null, "partitioned0", "replicated0"),
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT + 1, null,
                "partitioned1", "replicated1"),
            igfsConfiguration(IgfsIpcEndpointType.SHMEM, IgfsIpcEndpointConfiguration.DFLT_PORT + 2, null,
                "partitioned2", "replicated2"));

        G.start(cfg);

        T2<Integer, Integer> res = checkRegisteredIpcEndpoints();

        // 1 regular + 3 management TCP endpoins.
        assertEquals(4, res.get1().intValue());
        assertEquals(2, res.get2().intValue());
    }
}

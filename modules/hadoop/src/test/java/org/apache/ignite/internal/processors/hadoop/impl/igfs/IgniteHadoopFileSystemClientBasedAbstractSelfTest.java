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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * IGFS Hadoop file system Ignite client based IPC self test.
 */
public abstract class IgniteHadoopFileSystemClientBasedAbstractSelfTest extends IgniteHadoopFileSystemAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     */
    protected IgniteHadoopFileSystemClientBasedAbstractSelfTest(IgfsMode mode) {
        super(mode, true, true);
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String gridName) {
//        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();
//
//        endpointCfg.setType(IgfsIpcEndpointType.SHMEM);
//        endpointCfg.setPort(DFLT_IPC_PORT + getTestGridIndex(gridName));
//
//        return endpointCfg;
        return null;
    }

    @Override protected FileSystemConfiguration igfsConfiguration(String gridName) throws IgniteCheckedException {
        FileSystemConfiguration cfg = super.igfsConfiguration(gridName);

        cfg.setIpcEndpointEnabled(false);

        return cfg;
    }

    /**
     * @return Path to Ignite client node configuration.
     */
    protected abstract String getClientConfig();

    /** {@inheritDoc} */
    @Override protected Configuration configuration(String authority, boolean skipEmbed, boolean skipLocShmem) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://" + authority + "/");
        cfg.set("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl",
            org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);

        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);

        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP, authority), true);

        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_REMOTE_TCP, authority), true);

        cfg.setStrings(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_IGNITE_CFG_PATH, authority),
            getClientConfig());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testClientReconnect() throws Exception {
        Path filePath = new Path(PRIMARY_URI, "file1");

        final FSDataOutputStream s = fs.create(filePath); // Open the stream before stopping IGFS.

        try {
            stopNodes();

            startNodes(); // Start server again.

//            Thread.sleep(20000);

            // Check that client is again operational.
            assertTrue(fs.mkdirs(new Path(PRIMARY_URI, "dir1/dir2")));

//            // However, the streams, opened before disconnect, should not be valid.
//            GridTestUtils.assertThrows(log, new Callable<Object>() {
//                @Nullable @Override public Object call() throws Exception {
                    s.write("test".getBytes());

                    s.flush(); // Flush data to the broken output stream.

//                    return null;
//                }
//            }, IOException.class, null);

            assertTrue(fs.exists(filePath));

            System.out.println("+++ OKKK");
        }
        finally {
            U.closeQuiet(s); // Safety.
        }
    }


//    /** {@inheritDoc} */
//    @Override public void testInitialize() throws Exception {
//        // No-op.
//    }
//
//    /** {@inheritDoc} */
//    @Override public void testClientReconnect() throws Exception {
//        // No-op.
//    }
//
//    /** {@inheritDoc} */
//    @Override public void testClientReconnectMultithreaded() throws Exception {
//        // No-op.
//    }
}
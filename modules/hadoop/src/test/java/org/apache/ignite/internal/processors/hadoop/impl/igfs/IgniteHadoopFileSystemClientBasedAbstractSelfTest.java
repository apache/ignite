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

import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;

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
        return null;
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
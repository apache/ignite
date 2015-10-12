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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;

import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.IGFS_SCHEME;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.SECONDARY_CFG_PATH;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.configuration;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.mkUri;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.writeConfiguration;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Abstract test for Hadoop 1.0 file system stack.
 */
public abstract class Hadoop1DualAbstractTest extends IgfsDualAbstractSelfTest {
    /** Secondary grid name */
    private static final String GRID_NAME = "grid_secondary";

    /** Secondary file system name */
    private static final String IGFS_NAME = "igfs_secondary";

    /** Secondary file system REST endpoint port */
    private static final int PORT = 11500;

    /** Secondary file system REST endpoint configuration map. */
    private static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration() {{
        setType(IgfsIpcEndpointType.TCP);
        setPort(PORT);
    }};

    /** Secondary file system authority. */
    private static final String SECONDARY_AUTHORITY = IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + PORT;

    /** Secondary Fs configuration full path. */
    protected String secondaryConfFullPath;

    /** Secondary Fs URI. */
    protected String secondaryUri;

    /** Constructor. */
    public Hadoop1DualAbstractTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Creates secondary filesystems.
     * @return IgfsSecondaryFileSystem
     * @throws Exception On failure.
     */
    @Override protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        startUnderlying();

        prepareConfiguration();

        IgniteHadoopIgfsSecondaryFileSystem second =
            new IgniteHadoopIgfsSecondaryFileSystem(secondaryUri, secondaryConfFullPath);

        FileSystem fileSystem = second.fileSystem();

        igfsSecondary = new HadoopFileSystemUniversalFileSystemAdapter(fileSystem);

        return second;
    }

    /**
     * Starts underlying Ignite process.
     * @throws IOException On failure.
     */
    protected void startUnderlying() throws Exception {
        startGridWithIgfs(GRID_NAME, IGFS_NAME, PRIMARY, null, SECONDARY_REST_CFG);
    }

    /**
     * Prepares Fs configuration.
     * @throws IOException On failure.
     */
    protected void prepareConfiguration() throws IOException {
        Configuration secondaryConf = configuration(IGFS_SCHEME, SECONDARY_AUTHORITY, true, true);

        secondaryConf.setInt("fs.igfs.block.size", 1024);

        secondaryConfFullPath = writeConfiguration(secondaryConf, SECONDARY_CFG_PATH);

        secondaryUri = mkUri(IGFS_SCHEME, SECONDARY_AUTHORITY);
    }
}
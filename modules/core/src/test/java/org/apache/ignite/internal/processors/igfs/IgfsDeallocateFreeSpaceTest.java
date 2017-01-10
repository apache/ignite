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

import java.io.IOException;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.jetbrains.annotations.Nullable;

/**
 * Test to reproduce IGNITE-3400: after a writing to a file hits the IGFS size limits
 * the free space is calculated incorrectly and data blocks leaks.
 */
public class IgfsDeallocateFreeSpaceTest extends IgfsAbstractBaseSelfTest {
    /** Maximum amount of bytes that could be written to particular file. */
    private static final int IGFS_SIZE_LIMIT = 10_000_000;

    /** Small file size. */
    private static final int SMALL_FILE_SIZE = 4_000_000;

    /** Big file size. */
    private static final int BIG_FILE_SIZE = 9_000_000;

    /** Iterations. */
    private static final int ITERATIONS = 1000;

    /**
     *
     */
    public IgfsDeallocateFreeSpaceTest() {
        super(IgfsMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected FileSystemConfiguration getFileSystemConfiguration(String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) {
        FileSystemConfiguration cfg = super.getFileSystemConfiguration(igfsName, mode, secondaryFs, restCfg);

        cfg.setMaxSpaceSize(IGFS_SIZE_LIMIT);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeallocate() throws Exception {

        for (int i = 0; i < ITERATIONS; ++i) {
            byte[] small = new byte[SMALL_FILE_SIZE];

            createFile(igfs, new IgfsPath("/small"), true, small);

            byte[] big = new byte[BIG_FILE_SIZE];

            try {
                createFile(igfs, new IgfsPath("/big1"), true, big);
            } catch (IOException e) {
                // swallow.
            }

            igfs.format();

            if (IGFS_SIZE_LIMIT > freeSpace()) {
                dumpCache("MetaCache", getMetaCache(igfs));

                dumpCache("DataCache", getDataCache(igfs));

                assert false;
            }
        }
    }

    /**
     * @return IGFS free space.
     */
    private long freeSpace() {
        IgfsStatus s = igfs.globalSpace();

        return s.spaceTotal() - s.spaceUsed();
    }
}
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

import java.io.File;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.local.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Tests for PRIMARY mode.
 */
public class IgfsProxyToLocalFilesystemSelfTest extends IgfsProxySelfTest {
    private static final String FS_WORK_DIR = U.getIgniteHome() + File.separatorChar + "work"
        + File.separatorChar + "fs";

    /**
     * Creates secondary filesystems.
     * @return IgfsSecondaryFileSystem
     * @throws Exception On failure.
     */
    @Override protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        final File workDir = new File(FS_WORK_DIR);

        if (!workDir.exists())
            assert workDir.mkdirs();

        LocalIgfsSecondaryFileSystem second = new LocalIgfsSecondaryFileSystem();

        second.setWorkDirectory(workDir.getAbsolutePath());

        igfsSecondary = new IgfsLocalSecondaryFileSystemTestAdapter(workDir);

        return second;
    }

    /** The operation setTimes isn't supported in PROXY mode */
    @Override public void testSetTimes() throws Exception {
        return;
    }

    /** {@inheritDoc} */
    @Override protected boolean permissionsSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean propertiesSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean timesSupported() {
        return false;
    }

}
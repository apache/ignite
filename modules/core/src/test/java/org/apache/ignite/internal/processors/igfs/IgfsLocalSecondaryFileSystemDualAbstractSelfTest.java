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

import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.local.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;

/**
 * Abstract test for Hadoop 1.0 file system stack.
 */
public abstract class IgfsLocalSecondaryFileSystemDualAbstractSelfTest extends IgfsDualAbstractSelfTest {
    /** */
    private static final String FS_WORK_DIR = U.getIgniteHome() + File.separatorChar + "work"
        + File.separatorChar + "fs";

    /** Constructor.
     * @param mode IGFS mode.
     */
    public IgfsLocalSecondaryFileSystemDualAbstractSelfTest(IgfsMode mode) {
        super(mode);
    }

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
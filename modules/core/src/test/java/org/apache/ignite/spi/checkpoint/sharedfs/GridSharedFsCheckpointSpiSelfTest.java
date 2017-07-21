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

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.io.File;
import java.io.FileFilter;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.checkpoint.GridCheckpointSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Grid shared file system checkpoint SPI self test.
 */
@GridSpiTest(spi = SharedFsCheckpointSpi.class, group = "Checkpoint SPI")
public class GridSharedFsCheckpointSpiSelfTest extends GridCheckpointSpiAbstractTest<SharedFsCheckpointSpi> {
    /** */
    private static final String PATH = SharedFsCheckpointSpi.DFLT_DIR_PATH + "/" +
        GridSharedFsCheckpointSpiSelfTest.class.getSimpleName();

    /**
     * @return Paths.
     */
    @GridSpiTestConfig(setterName="setDirectoryPaths")
    public Collection<String> getDirectoryPaths() {
        return F.asList(PATH);
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterSpiStopped() throws Exception {
        File dir = new File(PATH);

        if (!dir.exists() || (!dir.isDirectory()))
            return;

        File[] files = dir.listFiles(new FileFilter() {
            @Override public boolean accept(File pathName) {
                return !pathName.isDirectory() && pathName.getName().endsWith(".gcp");
            }
        });

        if (files != null && files.length > 0)
            for (File file : files)
                file.delete();

        dir.delete();
    }
}
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
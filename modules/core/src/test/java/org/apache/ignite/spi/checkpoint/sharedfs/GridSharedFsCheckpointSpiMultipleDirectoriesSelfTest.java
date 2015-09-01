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
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.GridCheckpointTestState;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Tests multiple shared directories.
 */
@GridSpiTest(spi = SharedFsCheckpointSpi.class, group = "Checkpoint SPI")
public class GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest extends
    GridSpiAbstractTest<SharedFsCheckpointSpi> {
    /** */
    private static final String PATH1 = "cp/test-shared-fs1";

    /** */
    private static final String PATH2 = "cp/test-shared-fs2";

    /** */
    private static final String CHECK_POINT_KEY_PREFIX = "testCheckpoint";

    /**
     * @return Paths.
     */
    @GridSpiTestConfig(setterName="setDirectoryPaths")
    public Collection<String> getDirectoryPaths() {
        Collection<String> dirs = new ArrayList<>();

        dirs.add(PATH1);
        dirs.add(PATH2);

        return dirs;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleSharedDirectories() throws Exception {
        String data = "Test check point data.";

        GridCheckpointTestState state = new GridCheckpointTestState(data);

        getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state), 0, true);

        info("Saved check point [key=" + CHECK_POINT_KEY_PREFIX + ", data=" + state + ']');

        String curSpiPath1 = getSpi().getCurrentDirectoryPath();

        File folder1 = U.resolveWorkDirectory(curSpiPath1, false);

        assert folder1.exists() : "Checkpoint folder doesn't exist.";

        boolean rewritten = getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state),
            0, true);

        assert rewritten : "Check point was not rewritten.";

        info("Rewrite check point [key=" + CHECK_POINT_KEY_PREFIX + ", data=" + state + ']');

        String curSpiPath2 = getSpi().getCurrentDirectoryPath();

        File folder2 = U.resolveWorkDirectory(curSpiPath2, false);

        assert folder2.exists() : "Check point folder doesn't exist.";

        assert folder1.getAbsoluteFile().equals(folder2.getAbsoluteFile()) : "folder1 should be equal folder2.";

        U.delete(folder2);

        getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state), 0, true);

        info("Saved check point to other folder [key=" + CHECK_POINT_KEY_PREFIX + ", data=" + state + ']');

        String newCurSpiPath = getSpi().getCurrentDirectoryPath();

        File changedFolder = U.resolveWorkDirectory(newCurSpiPath, false);

        assert changedFolder.exists() : "Check point folder doesn't exist.";

        assert !folder2.getAbsolutePath().equals(changedFolder.getAbsolutePath()) : "Directories should not be equal.";

        U.delete(changedFolder);

        boolean error = false;

        // Try save after delete all directories.
        try {
            getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state), 0, true);
        }
        catch (IgniteException ignored) {
            error = true;
        }

        assert error : "Check point should not be saved.";
    }
}
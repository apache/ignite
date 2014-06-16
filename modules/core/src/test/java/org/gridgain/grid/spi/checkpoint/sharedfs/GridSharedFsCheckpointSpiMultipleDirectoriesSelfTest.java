/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.spi.*;

import java.io.*;
import java.util.*;

/**
 * Tests multiple shared directories.
 */
@GridSpiTest(spi = GridSharedFsCheckpointSpi.class, group = "Checkpoint SPI")
public class GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest extends
    GridSpiAbstractTest<GridSharedFsCheckpointSpi> {
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
        catch (GridException ignored) {
            error = true;
        }

        assert error : "Check point should not be saved.";
    }
}

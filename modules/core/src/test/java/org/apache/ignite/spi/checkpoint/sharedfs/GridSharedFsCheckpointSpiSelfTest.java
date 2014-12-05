/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.checkpoint.sharedfs;

import org.apache.ignite.spi.checkpoint.*;
import org.gridgain.grid.util.typedef.F;
import org.gridgain.testframework.junits.spi.*;
import java.io.*;
import java.util.Collection;

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

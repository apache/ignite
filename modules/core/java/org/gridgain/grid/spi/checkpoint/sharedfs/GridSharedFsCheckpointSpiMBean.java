// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;
import java.util.*;

/**
 * Management bean that provides general administrative and configuration information
 * about shared file system checkpoints.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean for shared file system based checkpoint SPI.")
public interface GridSharedFsCheckpointSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets path to the directory where all checkpoints are saved.
     *
     * @return Path to the checkpoints directory.
     */
    @GridMBeanDescription("Gets path to the directory where all checkpoints are saved.")
    public String getCurrentDirectoryPath();


    /**
     * Gets collection of all configured paths where checkpoints can be saved.
     *
     * @return Collection of all configured paths.
     */
    @GridMBeanDescription("Gets collection of all configured paths where checkpoints can be saved.")
    public Collection<String> getDirectoryPaths();
}

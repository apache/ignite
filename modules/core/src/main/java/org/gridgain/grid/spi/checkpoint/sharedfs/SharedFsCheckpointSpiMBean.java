/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

import java.util.*;

/**
 * Management bean that provides general administrative and configuration information
 * about shared file system checkpoints.
 */
@IgniteMBeanDescription("MBean for shared file system based checkpoint SPI.")
public interface SharedFsCheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets path to the directory where all checkpoints are saved.
     *
     * @return Path to the checkpoints directory.
     */
    @IgniteMBeanDescription("Gets path to the directory where all checkpoints are saved.")
    public String getCurrentDirectoryPath();


    /**
     * Gets collection of all configured paths where checkpoints can be saved.
     *
     * @return Collection of all configured paths.
     */
    @IgniteMBeanDescription("Gets collection of all configured paths where checkpoints can be saved.")
    public Collection<String> getDirectoryPaths();
}

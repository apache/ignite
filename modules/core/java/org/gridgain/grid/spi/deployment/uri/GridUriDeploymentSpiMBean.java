/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;
import java.util.*;

/**
 * Management bean for {@link GridUriDeploymentSpi}.
 */
@GridMBeanDescription("MBean that provides access to URI deployment SPI configuration.")
public interface GridUriDeploymentSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets temporary directory path.
     *
     * @return Temporary directory path.
     */
    @GridMBeanDescription("Temporary directory path.")
    public String getTemporaryDirectoryPath();

    /**
     * Gets list of URIs that are processed by SPI.
     *
     * @return List of URIs.
     */
    @GridMBeanDescription("List of URIs.")
    public List<String> getUriList();

    /**
     * Indicates if this SPI should check new deployment units md5 for redundancy.
     *
     * @return if files are ckecked for redundancy.
     */
    @GridMBeanDescription("Indicates if MD5 check is enabled.")
    public boolean isCheckMd5();
}

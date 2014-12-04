/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.s3;

import org.apache.ignite.mbean.*;
import org.gridgain.grid.spi.*;

/**
 * Management bean for {@link GridS3CheckpointSpi}.
 */
@IgniteMBeanDescription("MBean that provides access to S3 checkpoint SPI configuration.")
public interface GridS3CheckpointSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets S3 bucket name to use.
     *
     * @return S3 bucket name to use.
     */
    @IgniteMBeanDescription("S3 bucket name.")
    public String getBucketName();

    /**
     * @return S3 access key.
     */
    @IgniteMBeanDescription("S3 access key.")
    public String getAccessKey();

    /**
     * @return HTTP proxy host.
     */
    @IgniteMBeanDescription("HTTP proxy host.")
    public String getProxyHost();

    /**
     * @return HTTP proxy port
     */
    @IgniteMBeanDescription("HTTP proxy port.")
    public int getProxyPort();

    /**
     * @return HTTP proxy user name.
     */
    @IgniteMBeanDescription("HTTP proxy user name.")
    public String getProxyUsername();
}

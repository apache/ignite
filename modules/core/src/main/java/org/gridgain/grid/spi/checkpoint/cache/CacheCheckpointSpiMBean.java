package org.gridgain.grid.spi.checkpoint.cache;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management bean that provides general administrative and configuration information
 * about cache checkpoint SPI.
 *
 *
 */
@IgniteMBeanDescription("MBean provides information about cache checkpoint SPI.")
public interface CacheCheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets cache name to be used by this SPI..
     *
     * @return Cache name to be used by this SPI.
     */
    @IgniteMBeanDescription("Cache name to be used by this SPI.")
    public String getCacheName();
}

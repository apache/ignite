/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management MBean for {@link GridAdaptiveLoadBalancingSpi} SPI.
 */
@GridMBeanDescription("MBean that provides access to adaptive load balancing SPI configuration.")
public interface GridAdaptiveLoadBalancingSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets text description of current load probing implementation used.
     *
     * @return Text description of current load probing implementation used.
     */
    @GridMBeanDescription("Text description of current load probing implementation used.")
    public String getLoadProbeFormatted();
}

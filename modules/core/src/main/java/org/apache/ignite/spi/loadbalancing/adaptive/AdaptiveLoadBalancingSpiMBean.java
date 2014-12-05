/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.loadbalancing.adaptive;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management MBean for {@link AdaptiveLoadBalancingSpi} SPI.
 */
@IgniteMBeanDescription("MBean that provides access to adaptive load balancing SPI configuration.")
public interface AdaptiveLoadBalancingSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets text description of current load probing implementation used.
     *
     * @return Text description of current load probing implementation used.
     */
    @IgniteMBeanDescription("Text description of current load probing implementation used.")
    public String getLoadProbeFormatted();
}

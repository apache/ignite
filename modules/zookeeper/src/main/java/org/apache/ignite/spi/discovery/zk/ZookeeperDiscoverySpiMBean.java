/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;
import org.apache.ignite.spi.discovery.DiscoverySpiMBean;

/**
 * Management bean for {@link ZookeeperDiscoverySpi}.
 */
@MXBeanDescription("MBean provide access to Zookeeper-based discovery SPI.")
public interface ZookeeperDiscoverySpiMBean extends IgniteSpiManagementMBean, DiscoverySpiMBean {
    /**
     * Gets node join order.
     *
     * @return Node Join Order.
     */
    @MXBeanDescription("Node Join Order.")
    public long getNodeOrder();

    /**
     * Gets connection string used to connect to ZooKeeper cluster.
     *
     * @return Zk Connection String.
     */
    @MXBeanDescription("Zk Connection String.")
    public String getZkConnectionString();

    /**
     * Gets session timeout used by Zk client of local Ignite node.
     *
     * @return Zk Session Timeout.
     */
    @MXBeanDescription("Zk Session Timeout (milliseconds).")
    public long getZkSessionTimeout();

    /**
     * Gets session id of Zk client established with ZooKeeper cluster.
     *
     * @return Zk Session Id.
     */
    @MXBeanDescription("Zk Session Id.")
    public String getZkSessionId();

    /**
     * Gets number of communication resolver called.
     *
     * @return Number of communication resolved oparations.
     */
    @MXBeanDescription("Communication error resolver call count.")
    public long getCommErrorProcNum();

    /**
     * Gets root path in ZooKeeper cluster Zk client uses to put data to.
     *
     * @return Zk Root Path.
     */
    @MXBeanDescription("Zk Root Path.")
    public String getZkRootPath();
}

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

package org.apache.ignite.internal.client.router;

import java.util.Collection;
import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * MBean interface for TCP router.
 */
@MXBeanDescription("MBean for TCP router.")
public interface GridTcpRouterMBean {
    /**
     * Gets host for TCP binary protocol server.
     *
     * @return TCP host.
     */
    @MXBeanDescription("Host for TCP binary protocol server.")
    public String getHost();

    /**
     * Gets port for TCP binary protocol server.
     *
     * @return TCP port.
     */
    @MXBeanDescription("Port for TCP binary protocol server.")
    public int getPort();

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    @MXBeanDescription("Flag indicating whether or not SSL is enabled for incoming connections.")
    public boolean isSslEnabled();

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    @MXBeanDescription("Flag indicating whether or not remote clients are required to have a valid SSL certificate.")
    public boolean isSslClientAuth();

    /**
     * Gets list of server addresses where router's embedded client should connect.
     *
     * @return List of server addresses.
     */
    @MXBeanDescription("Gets list of server addresses where router's embedded client should connect.")
    public Collection<String> getServers();

    /**
     * Returns number of messages received by this router.
     * Note that this parameter has approximate value.
     *
     * @return Number of messages received by this router.
     */
    @MXBeanDescription("Number of messages received by this router.")
    public long getReceivedCount();

    /**
     * Returns number of responses returned by this router.
     * Note that this parameter has approximate value.
     *
     * @return Number of responses returned by this router.
     */
    @MXBeanDescription("Number of responses returned by this router.")
    public long getSendCount();
}
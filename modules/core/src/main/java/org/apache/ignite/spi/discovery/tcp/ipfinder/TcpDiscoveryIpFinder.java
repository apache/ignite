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

package org.apache.ignite.spi.discovery.tcp.ipfinder;

import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * IP finder interface for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 */
public interface TcpDiscoveryIpFinder {
    /**
     * Callback invoked when SPI context is initialized after {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi#spiStart(String)}
     * method is completed, SPI context can be stored for future access.
     *
     * @param spiCtx Spi context.
     * @throws IgniteSpiException In case of error.
     */
    public void onSpiContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException;

    /**
     * Callback invoked prior to stopping grid before SPI context is destroyed.
     * Note that invoking SPI context after this callback is complete is considered
     * illegal and may produce unknown results.
     */
    public void onSpiContextDestroyed();

    /**
     * Initializes addresses discovery SPI binds to.
     *
     * @param addrs Addresses discovery SPI binds to.
     * @throws IgniteSpiException In case of error.
     */
    public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Gets all addresses registered in this finder.
     *
     * @return All known addresses, potentially empty, but never {@code null}.
     * @throws IgniteSpiException In case of error.
     */
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException;

    /**
     * Checks whether IP finder is shared or not.
     * <p>
     * If this property is set to {@code true} then IP finder allows to add and remove
     * addresses in runtime and this is how, for example, IP finder should work in
     * Amazon EC2 environment or any other environment where IPs may not be known beforehand.
     * <p>
     * If this property is set to {@code false} then IP finder is immutable and all the addresses
     * should be listed in configuration before Ignite start. This is the most use case for IP finders
     * local to current VM. Since, usually such IP finders are created per each Ignite instance and
     * all the known IPs are listed right away, but there is also an option to make such IP finders shared
     * by setting this property to {@code true} and literally share it between local VM Ignite instances.
     * This way user does not have to list any IPs before start, instead all starting nodes add their addresses
     * to the finder, then get the registered addresses and continue with discovery procedure.
     *
     * @return {@code true} if IP finder is shared.
     */
    public boolean isShared();

    /**
     * Registers new addresses.
     * <p>
     * Implementation should accept duplicates quietly, but should not register address if it
     * is already registered.
     *
     * @param addrs Addresses to register. Not {@code null} and not empty.
     * @throws IgniteSpiException In case of error.
     */
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Unregisters provided addresses.
     * <p>
     * Implementation should accept addresses that are currently not
     * registered quietly (just no-op).
     *
     * @param addrs Addresses to unregister. Not {@code null} and not empty.
     * @throws IgniteSpiException In case of error.
     */
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Closes this IP finder and releases any system resources associated with it.
     */
    public void close();
}

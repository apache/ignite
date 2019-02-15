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

package org.apache.ignite.configuration;

import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;

/**
 * Provides resolution between external and internal addresses. In some cases
 * network routers are configured to perform address mapping between external
 * and internal networks and the same mapping must be available to SPIs
 * in Ignite that perform communication over IP protocols.
 */
public interface AddressResolver {
    /**
     * Maps internal address to a collection of external addresses.
     *
     * @param addr Internal (local) address.
     * @return Collection of addresses that this local address is "known" outside.
     *      Note that if there are more than one external network the local address
     *      can be mapped differently to each and therefore may need to return
     *      multiple external addresses.
     * @throws IgniteCheckedException Thrown if any exception occurs.
     */
    public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) throws IgniteCheckedException;
}
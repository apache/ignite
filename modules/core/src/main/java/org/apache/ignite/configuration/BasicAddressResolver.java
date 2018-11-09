/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Basic implementation of {@link AddressResolver}.
 * <p>
 * Allows to provide simple mapping between different address, which is useful
 * different parts of the cluster are located in different subnetworks and port
 * forwarding on the router is used for communication between them. Another
 * common case is Docker environment which can create a pair of public and private
 * address per container.
 * <p>
 * There are two different types of mapping supported by this implementation of
 * {@link AddressResolver}.
 * <p>
 * First type maps specific socket addresses (host and port pairs) are mapped to
 * each other. This is useful for port forwarding, where multiple nodes will
 * typically map to the same external address (router), but with different port
 * numbers. Here is the example:
 * <pre name="code" class="xml">
 * &lt;property name=&quot;addressResolver&quot;&gt;
 *     &lt;bean class=&quot;org.apache.ignite.configuration.BasicAddressResolver&quot;&gt;
 *         &lt;constructor-arg&gt;
 *             &lt;map&gt;
 *                 &lt;entry key=&quot;10.0.0.1:47100&quot; value=&quot;123.123.123.123:1111&quot;/&gt;
 *                 &lt;entry key=&quot;10.0.0.2:47100&quot; value=&quot;123.123.123.123:2222&quot;/&gt;
 *                 &lt;entry key=&quot;10.0.0.3:47100&quot; value=&quot;123.123.123.123:3333&quot;/&gt;
 *             &lt;/map&gt;
 *         &lt;/constructor-arg&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * Second type maps one host to another. In this case any internal address a node
 * is bound to will be mapped to the corresponding external host with the same
 * host number. Here is the example:
 * <pre name="code" class="xml">
 * &lt;property name=&quot;addressResolver&quot;&gt;
 *     &lt;bean class=&quot;org.apache.ignite.configuration.BasicAddressResolver&quot;&gt;
 *         &lt;constructor-arg&gt;
 *             &lt;map&gt;
 *                 &lt;entry key=&quot;10.0.0.1&quot; value=&quot;123.123.123.123&quot;/&gt;
 *             &lt;/map&gt;
 *         &lt;/constructor-arg&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * Here any port on {@code 10.0.0.1} will be mapped to the same port number on {@code 123.123.123.123}.
 * E.g., {@code 10.0.0.1:47100} will be mapped to {@code 123.123.123.123:47100}, and {@code 10.0.0.1:47500}
 * will be mapped to {@code 123.123.123.123:47500}.
 * <p>
 * Two types of mappings described above can be mixed within one address resolver.
 */
public class BasicAddressResolver implements AddressResolver {
    /** Address map. */
    @GridToStringInclude
    private final Map<InetAddress, InetAddress> inetAddrMap;

    /** Socket address map. */
    @GridToStringInclude
    private final Map<InetSocketAddress, InetSocketAddress> inetSockAddrMap;

    /**
     * Created the address resolver.
     *
     * @param addrMap Address mappings.
     * @throws UnknownHostException If any of the hosts can't be resolved.
     */
    public BasicAddressResolver(Map<String, String> addrMap) throws UnknownHostException {
        if (addrMap == null || addrMap.isEmpty())
            throw new IllegalArgumentException("At least one address mapping is required.");

        inetAddrMap = U.newHashMap(addrMap.size());
        inetSockAddrMap = U.newHashMap(addrMap.size());

        for (Map.Entry<String, String> e : addrMap.entrySet()) {
            String from = e.getKey();
            String to = e.getValue();

            if (F.isEmpty(from) || F.isEmpty(to))
                throw new IllegalArgumentException("Invalid address mapping: " + e);

            String[] fromArr = from.split(":");
            String[] toArr = to.split(":");

            assert fromArr.length > 0;
            assert toArr.length > 0;

            if (fromArr.length == 1) {
                if (toArr.length != 1)
                    throw new IllegalArgumentException("Invalid address mapping: " + e);

                inetAddrMap.put(InetAddress.getByName(fromArr[0]), InetAddress.getByName(toArr[0]));
            }
            else if (fromArr.length == 2) {
                if (toArr.length != 2)
                    throw new IllegalArgumentException("Invalid address mapping: " + e);

                inetSockAddrMap.put(new InetSocketAddress(fromArr[0], Integer.parseInt(fromArr[1])),
                    new InetSocketAddress(toArr[0], Integer.parseInt(toArr[1])));
            }
            else
                throw new IllegalArgumentException("Invalid address mapping: " + e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr)
        throws IgniteCheckedException {
        InetSocketAddress inetSockAddr = inetSockAddrMap.get(addr);

        if (inetSockAddr != null)
            return Collections.singletonList(inetSockAddr);

        InetAddress inetAddr = inetAddrMap.get(addr.getAddress());

        if (inetAddr != null)
            return Collections.singletonList(new InetSocketAddress(inetAddr, addr.getPort()));

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BasicAddressResolver.class, this);
    }
}

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
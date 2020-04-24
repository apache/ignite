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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Compute job to retrieve remote node endpoints.
 */
public class ClientClusterGroupGetNodeEndpointsJob implements IgniteCallable<ClientClusterGroupGetNodeEndpointsJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private Ignite ignite;

    /** <inheritdoc /> */
    @Override public ClientClusterGroupGetNodeEndpointsJobResult call() throws Exception {
        int port = ((IgniteEx)ignite).context().sqlListener().port();

        IgniteBiTuple<Collection<String>, Collection<String>> locAddrsAndHosts =
                IgniteUtils.resolveLocalAddresses(InetAddress.getByName("0.0.0.0"), true);

        Collection<NodeEndpoint> endpoints = new ArrayList<>(locAddrsAndHosts.get1().size());

        Iterator<String> ipIt = locAddrsAndHosts.get1().iterator();
        Iterator<String> hostIt = locAddrsAndHosts.get2().iterator();

        while (ipIt.hasNext() && hostIt.hasNext()) {
            String ip = ipIt.next();

            // TODO: We should return addresses that match IgniteConfiguration.localHost?
            if (InetAddress.getByName(ip).isLoopbackAddress())
                continue;

            String hostName = hostIt.next();

            endpoints.add(new NodeEndpoint(ip, hostName, port));
        }

        return new ClientClusterGroupGetNodeEndpointsJobResult(endpoints);
    }
}

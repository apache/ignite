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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import com.github.dockerjava.api.model.Network.Ipam;
import com.github.dockerjava.api.model.Network.Ipam.Config;

/** Ignite cluster container. */
public class IgniteClusterContainer implements Startable {
    /** Containers. */
    private final List<IgniteContainer> containers;

    /**
     * Network configured with a static subnet profile.
     */
    private final Network net = Network.builder()
        .createNetworkCmdModifier(cmd -> {
            Config ipamCfg = new Config()
                .withSubnet("172.31.0.0/16")
                .withGateway("172.31.0.1");

            Ipam ipam =new Ipam().withConfig(Collections.singletonList(ipamCfg));

            cmd.withIpam(ipam);
        })
        .build();

    /** @param commitHash Commit hash. */
    public IgniteClusterContainer(String commitHash, int size) {
        containers = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            String hostname = "node" + (1 + i);

            String staticIpAddress = "172.31.0." + (2 + i);

            IgniteContainer ignite = new IgniteContainer(commitHash, net, hostname, staticIpAddress, i+1);

            containers.add(ignite);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        for (IgniteContainer container : containers)
            container.start();

        containers.get(0).activateCluster(containers.size());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        for (IgniteContainer container : containers)
            container.stop();

        net.close();
    }

    /** */
    public int size() {
        return containers.size();
    }

    /** */
    public IgniteContainer getContainer(int idx) {
        return containers.get(idx);
    }

    /** */
    public void stopNode(int idx) {
        IgniteContainer container = containers.remove(idx);

        container.stop();
    }

    /** */
    public List<IgniteContainer> containers() {
        return containers;
    }

    /**
     * Collects and returns the dynamically mapped discovery addresses
     * for all nodes currently managed by this cluster container.
     * @return A collection of addresses formatted as "host:port".
     */
    public Collection<String> serverAddresses() {
        return containers.stream()
            .map(IgniteContainer::serverAddress)
            .collect(Collectors.toList());
    }
}

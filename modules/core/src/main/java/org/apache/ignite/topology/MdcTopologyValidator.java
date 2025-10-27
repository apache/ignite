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

package org.apache.ignite.topology;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Multi-Datacenter topology validator.
 * Performs data protection in case of DC failure.
 * Covered DCs MUST be specified via {@link MdcTopologyValidator#setDatacenters}
 * and main DC MUST be specified via {@link MdcTopologyValidator#setMainDatacenter} in case of even DC count.
 * When main datacenter is specified Topology Validator keeps cluster write accessed while main DC is visible,
 * otherwise DC majority check is used.
 * */
@IgniteExperimental
public class MdcTopologyValidator implements TopologyValidator {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Set<String> dcs;

    /** */
    private String mainDc;

    /** @param datacenters Datacenters.*/
    public void setDatacenters(Set<String> datacenters) {
        if (mainDc != null && datacenters.size() % 2 == 1)
            throw new IllegalArgumentException("Datacenters count must be even when main datacenter is set.");

        dcs = datacenters;
    }

    /** @param mainDatacenter Main datacenter.*/
    public void setMainDatacenter(String mainDatacenter) {
        if (mainDatacenter != null && dcs != null && dcs.size() % 2 == 1)
            throw new IllegalArgumentException("Datacenters count must be even when main datacenter is set.");

        mainDc = mainDatacenter;
    }

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        Stream<ClusterNode> servers = nodes.stream().filter(node -> !node.isClient());

        if (mainDc != null)
            return servers.anyMatch(n -> n.dataCenterId() != null && n.dataCenterId().equals(mainDc));

        long visible = servers.map(ClusterNode::dataCenterId).count();
        int half = dcs.size() / 2;

        return visible > half;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        MdcTopologyValidator validator = (MdcTopologyValidator)o;

        return Objects.equals(dcs, validator.dcs) && Objects.equals(mainDc, validator.mainDc);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(dcs, mainDc);
    }
}

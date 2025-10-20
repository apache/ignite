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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Multi-Datacenter topology validator.
 * Performs data protection in case of DC failure.
 * Covered DCs SHOULD be specified via {@link MdcTopologyValidator#setDatacenters}
 * and primary DC MAY be specified via {@link MdcTopologyValidator#setPrimaryDatacenter}.
 * When primary datacenter is specified Topology Validator keeps cluster write accessed while primary DC is visible,
 * otherwise DC majority check is used.
 * */
@IgniteExperimental
public class MdcTopologyValidator implements TopologyValidator {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<String> dcs;

    /** */
    private String primDc;

    /** @param datacenters Datacenters.*/
    public void setDatacenters(List<String> datacenters) {
        dcs = datacenters;
    }

    /** @param primaryDatacenter Primary datacenter.*/
    public void setPrimaryDatacenter(String primaryDatacenter) {
        primDc = primaryDatacenter;
    }

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        if (primDc != null) {
            return nodes.stream().anyMatch(n -> n.dataCenterId().equals(primDc));
        }

        List<String> visibleDcs = nodes.stream().map(ClusterNode::dataCenterId).collect(Collectors.toList());
        int visible = visibleDcs.size();
        int half = dcs.size() / 2;

        return visible > half;
    }
}

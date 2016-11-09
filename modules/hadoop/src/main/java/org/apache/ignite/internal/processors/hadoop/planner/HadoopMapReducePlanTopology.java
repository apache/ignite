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

package org.apache.ignite.internal.processors.hadoop.planner;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Map-reduce plan topology.
 */
public class HadoopMapReducePlanTopology {
    /** All groups. */
    private final List<HadoopMapReducePlanGroup> grps;

    /** Node ID to group map. */
    private final Map<UUID, HadoopMapReducePlanGroup> idToGrp;

    /** Host to group map. */
    private final Map<String, HadoopMapReducePlanGroup> hostToGrp;

    /**
     * Constructor.
     *
     * @param grps All groups.
     * @param idToGrp ID to group map.
     * @param hostToGrp Host to group map.
     */
    public HadoopMapReducePlanTopology(List<HadoopMapReducePlanGroup> grps,
        Map<UUID, HadoopMapReducePlanGroup> idToGrp, Map<String, HadoopMapReducePlanGroup> hostToGrp) {
        assert grps != null;
        assert idToGrp != null;
        assert hostToGrp != null;

        this.grps = grps;
        this.idToGrp = idToGrp;
        this.hostToGrp = hostToGrp;
    }

    /**
     * @return All groups.
     */
    public List<HadoopMapReducePlanGroup> groups() {
        return grps;
    }

    /**
     * Get group for node ID.
     *
     * @param id Node ID.
     * @return Group.
     */
    public HadoopMapReducePlanGroup groupForId(UUID id) {
        return idToGrp.get(id);
    }

    /**
     * Get group for host.
     *
     * @param host Host.
     * @return Group.
     */
    @Nullable public HadoopMapReducePlanGroup groupForHost(String host) {
        return hostToGrp.get(host);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopMapReducePlanTopology.class, this);
    }
}

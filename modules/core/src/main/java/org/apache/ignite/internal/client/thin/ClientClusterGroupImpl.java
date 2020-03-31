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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Implementation of {@link ClientClusterGroup}.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
class ClientClusterGroupImpl implements ClientClusterGroup {
    /** Node id's. */
    private final Collection<UUID> nodeIds;

    /**
     * @param ids Ids.
     */
    ClientClusterGroupImpl(Collection<UUID> ids) {
        nodeIds = ids;
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNodeIds(Collection<UUID> ids) {
        return new ClientClusterGroupImpl(new HashSet<>(ids));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNodeId(UUID id, UUID ... ids) {
        Collection<UUID> nodeIds = U.newHashSet(1 + (ids == null ? 0 : ids.length));

        nodeIds.add(id);

        if (ids != null)
            nodeIds.addAll(Arrays.asList(ids));

        return new ClientClusterGroupImpl(nodeIds);
    }

    /**
     * Gets node id's.
     */
    public Collection<UUID> nodeIds() {
        return nodeIds == null ? null : new HashSet<>(nodeIds);
    }
}

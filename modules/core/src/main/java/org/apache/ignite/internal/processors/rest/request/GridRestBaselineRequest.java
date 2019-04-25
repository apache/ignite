/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.request;

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid command topology request.
 */
public class GridRestBaselineRequest extends GridRestRequest {
    /** Topology version to set. */
    private Long topVer;

    /** Collection of consistent IDs to set. */
    private List<Object> consistentIds;

    /**
     * @return Topology version to set.
     */
    public Long topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version to set.
     */
    public void topologyVersion(Long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Collection of consistent IDs to set.
     */
    public List<Object> consistentIds() {
        return consistentIds;
    }

    /**
     * @param consistentIds New collection of consistent IDs to set.
     */
    public void consistentIds(List<Object> consistentIds) {
        this.consistentIds = consistentIds;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestBaselineRequest.class, this, super.toString());
    }
}

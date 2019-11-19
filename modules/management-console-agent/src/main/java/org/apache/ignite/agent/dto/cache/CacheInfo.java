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

package org.apache.ignite.agent.dto.cache;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * DTO for cache info.
 */
public class CacheInfo {
    /** Cache name. */
    private String name;

    /** Deployment id. */
    private IgniteUuid deploymentId;

    /** Cache group. */
    private String grp;

    /**
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Deployment id.
     */
    public IgniteUuid getDeploymentId() {
        return deploymentId;
    }

    /**
     * @param deploymentId Deployment id.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setDeploymentId(IgniteUuid deploymentId) {
        this.deploymentId = deploymentId;

        return this;
    }

    /**
     * @return Cache group.
     */
    public String getGroup() {
        return grp;
    }

    /**
     * @param grp Group.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setGroup(String grp) {
        this.grp = grp;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInfo.class, this);
    }
}

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

package org.apache.ignite.agent.dto.cluster;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for cluster info.
 */
public class ClusterInfo {
    /** Cluster id. */
    private UUID id;

    /** Cluster tag. */
    private String tag;

    /** Baseline parameters. */
    private BaselineInfo baselineParameters;

    /** Is active. */
    private boolean isActive;

    /** Is persistence enabled. */
    private boolean isPersistenceEnabled;

    /** Features. */
    private Set<String> features = Collections.emptySet();

    /**
     * Default constructor.
     */
    public ClusterInfo() {
        // No-op.
    }

    /**
     * @param id Cluster ID.
     * @param tag Cluster tag.
     */
    public ClusterInfo(UUID id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * @return Cluster id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Id.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setId(UUID id) {
        this.id = id;

        return this;
    }

    /**
     * @return Cluster tag.
     */
    public String getTag() {
        return tag;
    }

    /**
     * @param tag Cluster tag.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setTag(String tag) {
        this.tag = tag;

        return this;
    }

    /**
     * @return Baseline parameters.
     */
    public BaselineInfo getBaselineParameters() {
        return baselineParameters;
    }

    /**
     * @param baselineParameters Baseline parameters.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setBaselineParameters(BaselineInfo baselineParameters) {
        this.baselineParameters = baselineParameters;

        return this;
    }

    /**
     * @return Active status.
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * @param active Active.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setActive(boolean active) {
        isActive = active;

        return this;
    }

    /**
     * @return @{code True} if cluster running in memory mode.
     */
    public boolean isPersistenceEnabled() {
        return isPersistenceEnabled;
    }

    /**
     * @param persistenceEnabled Is persistence enabled.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setPersistenceEnabled(boolean persistenceEnabled) {
        isPersistenceEnabled = persistenceEnabled;

        return this;
    }

    /**
     * @return Supported cluster features.
     */
    public Set<String> getFeatures() {
        return features;
    }

    /**
     * @param features Features.
     * @return {@code This} for chaining method calls.
     */
    public ClusterInfo setFeatures(Set<String> features) {
        this.features = features;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterInfo.class, this);
    }
}

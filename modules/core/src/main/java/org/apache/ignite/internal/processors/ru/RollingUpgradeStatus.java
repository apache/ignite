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
package org.apache.ignite.internal.processors.ru;

import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a cluster-wide state of Rolling Upgrade process.
 */
public interface RollingUpgradeStatus extends Serializable {
    /**
     * Returns {@code true} if Rolling Upgrade is enabled and is in progress.
     *
     * @return {@code true} if Rolling Upgrade is enabled.
     */
    public boolean isEnabled();

    /**
     * @return {@code true} if strict mode is enabled.
     */
    public boolean isForcedModeEnabled();

    /**
     * Returns the version that is used as starting point for Rolling Upgrade.
     *
     * @return Initial version.
     */
    public IgniteProductVersion getInitialVersion();

    /**
     * Returns the resulting version.
     * The returned value can be {@code null} if Rolling Upgrade is not in progress
     * or resulting version is not determined yet.
     *
     * @return Resulting version.
     */
    public @Nullable IgniteProductVersion getUpdateVersion();

    /**
     * Returns a set of features that is supported by all nodes in the cluster.
     *
     * @return Feature set supported by all cluster nodes.
     */
    public Set<IgniteFeatures> getSupportedFeatures();
}

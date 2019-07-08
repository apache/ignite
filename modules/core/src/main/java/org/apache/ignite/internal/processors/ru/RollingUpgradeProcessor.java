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

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessor;

import static org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult.Result.FAIL;

/**
 * Defines public API for Rolling Upgrade process.
 */
public interface RollingUpgradeProcessor extends GridProcessor {
    /**
     * Enables or disables rolling upgrade mode.
     *
     * @param enable {@code true} in order to enable rolling upgrade mode.
     */
    public default RollingUpgradeModeChangeResult setMode(boolean enable) {
        return new RollingUpgradeModeChangeResult(
            FAIL,
            new UnsupportedOperationException("Rolling Upgrade is not supported."),
            getStatus());
    }

    /**
     * Returns cluster-wide status of Rolling Upgrade process.
     *
     * @return status of Rolling Upgrade process.
     */
    public default RollingUpgradeStatus getStatus() {
        return new IgniteRollingUpgradeStatus(
            false,
            false,
            null,
            null,
            new HashSet<>(Arrays.asList(IgniteFeatures.values())));
    }

    /**
     * Enables forced mode of rolling upgrade.
     * This means that the strict version checking of the node should not be used and therefore
     * this mode allows to coexist more than two versions of Ignite nodes in the cluster.
     */
    public default RollingUpgradeModeChangeResult enableForcedMode() {
        return new RollingUpgradeModeChangeResult(
            FAIL,
            new UnsupportedOperationException("Forced mode of Rolling Upgrade is not supported."),
            getStatus());
    }
}

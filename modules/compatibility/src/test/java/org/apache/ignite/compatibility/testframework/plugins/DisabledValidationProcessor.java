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

package org.apache.ignite.compatibility.testframework.plugins;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult.Status.FAIL;

/**
 * Disabled node validation.
 */
public class DisabledValidationProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /**
     * @param ctx Kernal context.
     */
    public DisabledValidationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeModeChangeResult setMode(boolean enable) {
        ClusterNode locNode = ctx.discovery().localNode();

        return new RollingUpgradeModeChangeResult(
            FAIL,
            new UnsupportedOperationException("Local node does not support Rolling Upgrade "
                + "[locNodeId=" + locNode.id() + ", locNodeAddrs=" + U.addressesAsString(locNode)
                + ", locBuildVer=" + locNode.attribute(ATTR_BUILD_VER)
            ));
    }

    /** {@inheritDoc} */
    @Override public void enableForcedMode() {
        throw new UnsupportedOperationException("OS nodes do not support Rolling Upgrade.");
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeStatus getStatus() {
        return new RollingUpgradeStatus(
            false,
            IgniteProductVersion.fromString(ctx.discovery().localNode().attribute(ATTR_BUILD_VER)),
            null,
            true,
            new HashSet<>(Arrays.asList(IgniteFeatures.values())));
    }
}

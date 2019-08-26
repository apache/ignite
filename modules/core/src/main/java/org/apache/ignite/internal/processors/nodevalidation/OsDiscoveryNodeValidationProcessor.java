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

package org.apache.ignite.internal.processors.nodevalidation;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.ru.IgniteRollingUpgradeStatus;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult.Result.FAIL;

/**
 * Node validation.
 */
public class OsDiscoveryNodeValidationProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /** Default value for rolling upgrade status. */
    private static final RollingUpgradeStatus DEFAULT_ROLLING_UPGRADE_STATUS = new IgniteRollingUpgradeStatus(
        false,
        false,
        IgniteProductVersion.fromString(IgniteVersionUtils.VER_STR),
        null,
        new HashSet<>(Arrays.asList(IgniteFeatures.values())));

    /**
     * @param ctx Kernal context.
     */
    public OsDiscoveryNodeValidationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        ClusterNode locNode = ctx.discovery().localNode();

        // Check version.
        String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
        String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

        if (!F.eq(rmtBuildVer, locBuildVer)) {
            // OS nodes don't support rolling updates.
            if (!locBuildVer.equals(rmtBuildVer)) {
                String errMsg = "Local node and remote node have different version numbers " +
                    "(node will not join, Ignite does not support rolling updates, " +
                    "so versions must be exactly the same) " +
                    "[locBuildVer=" + locBuildVer + ", rmtBuildVer=" + rmtBuildVer +
                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(node) +
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + node.id() + ']';

                LT.warn(log, errMsg);

                // Always output in debug.
                if (log.isDebugEnabled())
                    log.debug(errMsg);

                return new IgniteNodeValidationResult(node.id(), errMsg);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeModeChangeResult setMode(boolean enable) {
        ClusterNode locNode = ctx.discovery().localNode();

        return new RollingUpgradeModeChangeResult(
            FAIL,
            new UnsupportedOperationException("Local node does not support Rolling Upgrade "
                + "[locNodeId=" + locNode.id() + ", locNodeAddrs=" + U.addressesAsString(locNode)
                + ", locBuildVer=" + locNode.attribute(ATTR_BUILD_VER) + ']'),
            getStatus());
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeModeChangeResult enableForcedMode() {
        ClusterNode locNode = ctx.discovery().localNode();

        return new RollingUpgradeModeChangeResult(
            FAIL,
            new UnsupportedOperationException("Local node does not support forced mode of Rolling Upgrade "
                + "[locNodeId=" + locNode.id() + ", locNodeAddrs=" + U.addressesAsString(locNode)
                + ", locBuildVer=" + locNode.attribute(ATTR_BUILD_VER)
            ),
            getStatus());
    }

    /** {@inheritDoc} */
    @Override public RollingUpgradeStatus getStatus() {
        return DEFAULT_ROLLING_UPGRADE_STATUS;
    }
}

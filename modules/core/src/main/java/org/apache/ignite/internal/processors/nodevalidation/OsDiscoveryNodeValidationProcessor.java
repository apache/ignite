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

package org.apache.ignite.internal.processors.nodevalidation;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;

/**
 * Node validation.
 */
public class OsDiscoveryNodeValidationProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /** Enables version check for rolling upgrade. */
    @SystemProperty(value = "Enables version check for rolling upgrade.")
    public static final String IGNITE_ROLLING_UPGRADE_VERSION_CHECK = "IGNITE.ROLLING.UPGRADE.VERSION.CHECK";

    /** */
    private static final int MAX_MINOR_DIFF = 1;

    /**
     * @param ctx Kernal context.
     */
    public OsDiscoveryNodeValidationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        ClusterNode locNode = ctx.discovery().localNode();

        String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
        String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

        IgniteProductVersion locVer = IgniteProductVersion.fromString(locBuildVer);
        IgniteProductVersion rmtVer = IgniteProductVersion.fromString(rmtBuildVer);

        if (!isRollingUpgradeEligible(locVer, rmtVer)) {
            String errMsg = "Remote node rejected due to incompatible version for cluster join.\n"
                + "Remote node info:\n"
                + "  - Version     : " + rmtBuildVer + "\n"
                + "  - Addresses   : " + U.addressesAsString(node) + "\n"
                + "  - Node ID     : " + node.id() + "\n"
                + "Local node info:\n"
                + "  - Version     : " + locBuildVer + "\n"
                + "  - Addresses   : " + U.addressesAsString(locNode) + "\n"
                + "  - Node ID     : " + locNode.id() + "\n"
                + "Allowed versions for joining:\n"
                + "  - " + locVer.major() + '.' + locVer.minor() + ".X\n"
                + "  - " + locVer.major() + '.' + (locVer.minor() + 1) + ".X\n"
                + "  - " + locVer.major() + '.' + (locVer.minor() - 1) + ".X";

            LT.warn(log, errMsg);

            if (log.isDebugEnabled())
                log.debug(errMsg);

            return new IgniteNodeValidationResult(node.id(), errMsg);
        }

        return null;
    }

    /**
     * Determines whether the remote node's version is eligible for rolling upgrade with the local node.
     * <p>
     * A remote version is considered eligible if:
     * <ul>
     *   <li>It exactly matches the local version; or</li>
     *   <li>It has the same major version as the local node and the difference in minor versions is within {@link #MAX_MINOR_DIFF}.</li>
     * </ul>
     *
     * @param locVer Local node's Ignite product version.
     * @param rmtVer Remote node's Ignite product version.
     * @return {@code true} if the remote version is compatible and eligible for rolling upgrade, {@code false} otherwise.
     */
    private boolean isRollingUpgradeEligible(IgniteProductVersion locVer, IgniteProductVersion rmtVer) {
        if (locVer.major() != rmtVer.major())
            return false;

        if (locVer.minor() == rmtVer.minor())
            return true;

        boolean enableCheck = IgniteSystemProperties.getBoolean(IGNITE_ROLLING_UPGRADE_VERSION_CHECK);
        if (!enableCheck)
            return false;

        Set<Byte> versions = ctx.discovery().allNodes().stream()
            .map(node -> IgniteProductVersion.fromString(node.attribute(ATTR_BUILD_VER)).minor())
            .collect(Collectors.toSet());

        if (versions.contains(rmtVer.minor()))
            return true;

        if (versions.size() > 1)
            return false;

        return locVer.major() == rmtVer.major() && Math.abs(locVer.minor() - rmtVer.minor()) <= MAX_MINOR_DIFF;
    }
}

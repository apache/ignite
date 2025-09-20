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
    public static final String IGNITE_ROLLING_UPGRADE_VERSION_CHECK = "IGNITE_ROLLING_UPGRADE_VERSION_CHECK";

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

        Set<Byte> allowedMinors = allowedMinorVersions(locVer);

        if (locVer.major() != rmtVer.major() || !allowedMinors.contains(rmtVer.minor())) {
            StringBuilder errMsg = new StringBuilder("Remote node rejected due to incompatible version for cluster join.\n"
                + "Remote node info:\n"
                + "  - Version     : " + rmtBuildVer + "\n"
                + "  - Addresses   : " + U.addressesAsString(node) + "\n"
                + "  - Node ID     : " + node.id() + "\n"
                + "Local node info:\n"
                + "  - Version     : " + locBuildVer + "\n"
                + "  - Addresses   : " + U.addressesAsString(locNode) + "\n"
                + "  - Node ID     : " + locNode.id() + "\n"
                + "Allowed versions for joining:");

            allowedMinors.stream().sorted().forEach(minor -> errMsg.append("\n")
                .append(" - ").append(locVer.major()).append('.').append(minor).append(".X"));

            LT.warn(log, errMsg.toString());

            if (log.isDebugEnabled())
                log.debug(errMsg.toString());

            return new IgniteNodeValidationResult(node.id(), errMsg.toString());
        }

        return null;
    }

    /**
     * A remote version is considered allowed for joining if:
     * <ul>
     *   <li>It exactly matches the local version; or</li>
     *   <li>It has the same major version as the local node and the minor versions differ by at most one.</li>
     * </ul>
     * @param locVer Local version.
     */
    private Set<Byte> allowedMinorVersions(IgniteProductVersion locVer) {
        Set<Byte> minors = ctx.discovery().allNodes().stream()
            .map(node -> IgniteProductVersion.fromString(node.attribute(ATTR_BUILD_VER)).minor())
            .collect(Collectors.toSet());

        if (minors.size() == 1 && IgniteSystemProperties.getBoolean(IGNITE_ROLLING_UPGRADE_VERSION_CHECK)) {
            byte base = locVer.minor();
            minors.add((byte)(base + 1));
            minors.add((byte)(base - 1));
        }

        return minors;
    }
}

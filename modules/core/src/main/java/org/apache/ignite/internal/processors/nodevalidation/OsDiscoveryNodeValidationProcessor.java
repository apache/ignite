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

import java.util.concurrent.atomic.AtomicReference;
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
    /** */
    private static final int MAX_MINOR_DIFF = 1;

    /** */
    private final AtomicReference<IgniteProductVersion> rmtVerAllowedRef = new AtomicReference<>();

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

        assert locBuildVer != null : ATTR_BUILD_VER + " is not set on local node!";
        assert rmtBuildVer != null : ATTR_BUILD_VER + " is not set on remote node!";

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
                + "  - " + locVer.major() + '.' + locVer.minor() + ".X";

            IgniteProductVersion curRmtVerAllowed = rmtVerAllowedRef.get();

            if (curRmtVerAllowed == null) {
                errMsg += "\n  - " + locVer.major() + '.' + (locVer.minor() + 1) + ".X";
                errMsg += "\n  - " + locVer.major() + '.' + (locVer.minor() - 1) + ".X";
            }

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
     * If the version is deemed eligible, this method attempts to store the remote version for future validation.
     *
     * @param locVer Local node's Ignite product version.
     * @param rmtVer Remote node's Ignite product version.
     * @return {@code true} if the remote version is compatible and eligible for rolling upgrade, {@code false} otherwise.
     */
    private boolean isRollingUpgradeEligible(IgniteProductVersion locVer, IgniteProductVersion rmtVer) {
        if (locVer.major() == rmtVer.major() && locVer.minor() == rmtVer.minor())
            return true;

        boolean isEligible = locVer.major() == rmtVer.major() && Math.abs(locVer.minor() - rmtVer.minor()) <= MAX_MINOR_DIFF;

        if (isEligible)
            return updateEligibleRemoteVersion(rmtVer);

        return false;
    }

    /**
     * Atomically updates the allowed remote version for rolling upgrade, if not already set.
     * <p>
     * This method ensures that only one remote version is accepted as eligible across multiple threads.
     * If the reference is already set, it checks whether the incoming version matches the stored one.
     *
     * @param rmtVer Remote node's Ignite product version to check or set.
     * @return {@code true} if the version was successfully set or matches the already accepted version;
     *         {@code false} if the version conflicts with an already accepted different version.
     */
    private boolean updateEligibleRemoteVersion(IgniteProductVersion rmtVer) {
        while (true) {
            IgniteProductVersion curRmtVerAllowed = rmtVerAllowedRef.get();

            if (curRmtVerAllowed == null) {
                if (rmtVerAllowedRef.compareAndSet(null, rmtVer))
                    return true;
            }
            else
                return curRmtVerAllowed.major() == rmtVer.major() && curRmtVerAllowed.minor() == rmtVer.minor();
        }
    }
}

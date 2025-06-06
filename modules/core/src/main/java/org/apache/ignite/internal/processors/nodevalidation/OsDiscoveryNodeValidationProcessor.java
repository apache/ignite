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
    private static final int MAX_VER_DIFF_FOR_RU = 2;

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
            String errMsg = "Remote node [rmtBuildVer=" + rmtBuildVer + ", rmtNodeAddrs=" + U.addressesAsString(node) +
                ", rmtNodeId=" + node.id() + "] rejected: Incompatible version for cluster join. Allowed major version " +
                "range: " + locVer.major() + '.' + (locVer.minor() - 2) + '.' + locVer.maintenance() +
                " to " + locVer.major() + '.' + (locVer.minor() + 2) + '.' + locVer.maintenance() +
                " [locBuildVer=" + locBuildVer + ", locNodeAddrs=" + U.addressesAsString(locNode) +
                ", locNodeId=" + locNode.id() + ']';

            LT.warn(log, errMsg);

            if (log.isDebugEnabled())
                log.debug(errMsg);

            return new IgniteNodeValidationResult(node.id(), errMsg);
        }

        return null;
    }

    /**
     * Checks whether remote node is eligible for rolling upgrade.
     *
     * @param locVer - Ignite build version for local node.
     * @param rmtVer - Ignite build version for remote node.
     * @return True - if remote node is eligible for rolling upgrade, thus can enter the cluster. False - otherwise.
     */
    private boolean isRollingUpgradeEligible(IgniteProductVersion locVer, IgniteProductVersion rmtVer) {
        return Math.abs(locVer.minor() - rmtVer.minor()) <= MAX_VER_DIFF_FOR_RU;
    }
}

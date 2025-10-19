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
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
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

        IgniteProductVersion rmtVer = IgniteProductVersion.fromString(rmtBuildVer);

        IgnitePair<IgniteProductVersion> pair = ctx.rollingUpgrade().versions();

        if (pair == null) {
            pair = F.pair(IgniteProductVersion.fromString(locBuildVer), null);
        }

        IgniteProductVersion curVer = pair.get1();
        IgniteProductVersion targetVer = pair.get2();

        if (versionsMatch(rmtVer, curVer) || versionsMatch(rmtVer, targetVer))
            return null;

        String errMsg = "Remote node rejected due to incompatible version for cluster join.\n"
            + "Remote node info:\n"
            + "  - Version     : " + rmtBuildVer + "\n"
            + "  - Addresses   : " + U.addressesAsString(node) + "\n"
            + "  - Node ID     : " + node.id() + "\n"
            + "Local node info:\n"
            + "  - Version     : " + locBuildVer + "\n"
            + "  - Addresses   : " + U.addressesAsString(locNode) + "\n"
            + "  - Node ID     : " + locNode.id() + "\n"
            + "Allowed versions for joining: \n"
            + " - " + curVer.major() + "." + curVer.minor() + "." + curVer.maintenance()
            + (targetVer == null || targetVer.equals(curVer) ? "" :
            "\n - " + targetVer.major() + "." + targetVer.minor() + "." + targetVer.maintenance());

        LT.warn(log, errMsg);

        if (log.isDebugEnabled())
            log.debug(errMsg);

        return new IgniteNodeValidationResult(node.id(), errMsg);
    }

    /** Checks if versions have same major, minor and maintenance versions. */
    private boolean versionsMatch(IgniteProductVersion ver1, IgniteProductVersion ver2) {
        if (ver1 == null || ver2 == null)
            return false;

        return ver1.major() == ver2.major() && ver1.minor() == ver2.minor() && ver1.maintenance() == ver2.maintenance();
    }
}

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
import org.apache.ignite.internal.processors.configuration.distributed.DistributedIntegerProperty;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedIntegerProperty.detachedIntegerProperty;

/**
 * Node validation.
 */
public class OsDiscoveryNodeValidationProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /** */
    public static final String ROLL_UP_VERSION_CHECK = "rolling.upgrade.version.check";

    /** */
    private final DistributedIntegerProperty rollUpVerCheck = detachedIntegerProperty(ROLL_UP_VERSION_CHECK,
        "Distributed property that holds the rolling upgrade minor version check value. If not set or set to -1, rolling upgrade is considered disabled.");

    /**
     * @param ctx Kernal context.
     */
    public OsDiscoveryNodeValidationProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor()
            .registerDistributedConfigurationListener(dispatcher -> {
                rollUpVerCheck.addListener((name, oldVal, newVal) -> {
                    if (log.isInfoEnabled()) {
                        log.info(format("Distributed property '%s' was changed from '%s' to '%s'.",
                            name, oldVal, newVal));
                    }

                    if (newVal == null)
                        log.warning("Rolling upgrade version check was disabled.");
                });

                dispatcher.registerProperty(rollUpVerCheck);
            });

    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        ClusterNode locNode = ctx.discovery().localNode();

        String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
        String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

        IgniteProductVersion locVer = IgniteProductVersion.fromString(locBuildVer);
        IgniteProductVersion rmtVer = IgniteProductVersion.fromString(rmtBuildVer);

        Integer rollUpVerCheck = this.rollUpVerCheck.get();

        if (rmtVer.major() == locVer.major() &&
            (rmtVer.minor() == locVer.minor() || rollUpVerCheck != null && rollUpVerCheck != -1 && rmtVer.minor() == rollUpVerCheck))
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
            + "Allowed versions for joining:\n"
            + "  - " + locVer.major() + '.' + locVer.minor() + ".X"
            + (rollUpVerCheck != null && rollUpVerCheck != -1 ? "\n  - " + locVer.major() + '.' + rollUpVerCheck + ".X" : "");

        LT.warn(log, errMsg);

        if (log.isDebugEnabled())
            log.debug(errMsg);

        return new IgniteNodeValidationResult(node.id(), errMsg);

    }
}

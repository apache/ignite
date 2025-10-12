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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
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
    /** Key for the distributed property that holds current and target versions. */
    public static final String ROLL_UP_VERSIONS = "rolling.upgrade.versions";

    /** Distributed property that holds current and target version. */
    private final AtomicReference<IgnitePair<IgniteProductVersion>> verPairHolder = new AtomicReference<>();

    /**
     * @param ctx Kernal context.
     */
    public OsDiscoveryNodeValidationProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                try {
                    IgnitePair<IgniteProductVersion> pair = metastorage.read(ROLL_UP_VERSIONS);

                    if (verPairHolder.compareAndSet(null, pair) && log.isDebugEnabled())
                        log.debug("Read current and target versions from metastore: " + pair);
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Could not read current and target versions: " + e.getMessage());
                }

                metastorage.listen(ROLL_UP_VERSIONS::equals, (key, oldVal, newVal) ->
                    verPairHolder.compareAndSet((IgnitePair<IgniteProductVersion>)oldVal, (IgnitePair<IgniteProductVersion>)newVal)
                );
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        ClusterNode locNode = ctx.discovery().localNode();

        String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
        String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

        IgniteProductVersion rmtVer = IgniteProductVersion.fromString(rmtBuildVer);

        IgnitePair<IgniteProductVersion> pair = verPairHolder.get();

        if (pair == null) {
            IgniteProductVersion locVer = IgniteProductVersion.fromString(locBuildVer);
            pair = F.pair(locVer, locVer);
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

    /** Checks if versions has same major, minor and maintenance versions. */
    private boolean versionsMatch(IgniteProductVersion locVer, IgniteProductVersion rmtVer) {
        if (locVer == null || rmtVer == null)
            return false;

        return locVer.major() == rmtVer.major() && locVer.minor() == rmtVer.minor() && locVer.maintenance() == rmtVer.maintenance();
    }
}

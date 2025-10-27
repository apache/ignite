/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNodesRing;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX;

/** Rolling upgrade processor. Manages current and target versions of cluster. */
public class RollingUpgradeProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /** Key for the distributed property that holds current and target versions. */
    private static final String ROLLING_UPGRADE_VERSIONS_KEY = IGNITE_INTERNAL_KEY_PREFIX + "rolling.upgrade.versions";

    /** Metastorage with the write access. */
    @Nullable private volatile DistributedMetaStorage metastorage;

    /** TCP discovery nodes ring. */
    private TcpDiscoveryNodesRing ring;

    /** Last joining node. */
    private ClusterNode lastJoiningNode;

    /** Last joining node timestamp. */
    private long lastJoiningNodeTimestamp;

    /** Lock for synchronization between tcp-disco-msg-worker thread and management operations. */
    private final Object lock = new Object();

    /** Pair with current and target versions. */
    private volatile IgnitePair<IgniteProductVersion> verPair = null;

    /**
     * @param ctx Context.
     */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                RollingUpgradeProcessor.this.metastorage = metastorage;
            }

            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                try {
                    verPair = metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                metastorage.listen(ROLLING_UPGRADE_VERSIONS_KEY::equals, (key, oldVal, newVal) -> {
                    verPair = (IgnitePair<IgniteProductVersion>)newVal;
                });
            }
        });
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        synchronized (lock) {
            lastJoiningNode = node;

            lastJoiningNodeTimestamp = U.currentTimeMillis();
        }

        ClusterNode locNode = ctx.discovery().localNode();

        String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
        String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

        IgniteProductVersion rmtVer = IgniteProductVersion.fromString(rmtBuildVer);

        IgnitePair<IgniteProductVersion> pair = verPair;

        if (pair == null)
            pair = F.pair(IgniteProductVersion.fromString(locBuildVer), null);

        IgniteProductVersion curVer = pair.get1();
        IgniteProductVersion targetVer = pair.get2();

        if (validateVersionsForJoining(rmtVer, curVer) || validateVersionsForJoining(rmtVer, targetVer))
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

    /**
     * Enables rolling upgrade with specified target version.
     * This method can only be called on the coordinator node with {@link TcpDiscoverySpi}.
     *
     * @param target Target version.
     * @throws IgniteCheckedException If current and target versions are not compatible
     * or if node is not coordinator or if discovery SPI is not {@link TcpDiscoverySpi}.
     * @throws NullPointerException If metastorage is not available.
     */
    public void enable(IgniteProductVersion target) throws IgniteCheckedException {
        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            throw new IgniteCheckedException("Rolling upgrade can be enabled only on coordinator node");

        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (!(ctx.config().getDiscoverySpi() instanceof TcpDiscoverySpi))
            throw new IgniteCheckedException("Rolling upgrade is supported only with TCP discovery SPI");

        IgnitePair<IgniteProductVersion> oldVerPair = verPair;
        if (oldVerPair != null)
            throw new IgniteCheckedException("Rolling upgrade is already enabled with a different current and target version: " +
                oldVerPair.get1() + " , " + oldVerPair.get2());

        String curBuildVer = ctx.discovery().localNode().attribute(ATTR_BUILD_VER);
        IgniteProductVersion curVer = IgniteProductVersion.fromString(curBuildVer);

        if (!checkVersionsForEnabling(curVer, target))
            return;

        IgnitePair<IgniteProductVersion> newPair = F.pair(curVer, target);

        if (!metastorage.compareAndSet(ROLLING_UPGRADE_VERSIONS_KEY, null, newPair)) {
            oldVerPair = metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);

            if (newPair.equals(oldVerPair))
                return;

            if (oldVerPair == null)
                throw new IgniteCheckedException("Rolling upgrade is already disabled");

            throw new IgniteCheckedException("Rolling upgrade is already enabled with a different current and target version: " +
                oldVerPair.get1() + " , " + oldVerPair.get2());
        }

        verPair = newPair;

        if (log.isInfoEnabled())
            log.info("Rolling upgrade enabled [current=" + curVer + ", target=" + target + ']');
    }

    /**
     * Disables rolling upgrade.
     * This method can only be called on the coordinator node.
     *
     * @throws IgniteCheckedException If cluster has two or more nodes with different versions or if node is not coordinator
     * or if rolling upgrade is already disabled.
     * @throws NullPointerException If metastorage is not available.
     */
    public void disable() throws IgniteCheckedException {
        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            throw new IgniteCheckedException("Rolling upgrade can be disabled only on coordinator node");

        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (verPair == null)
            throw new IgniteCheckedException("Rolling upgrade is already disabled");

        IgnitePair<IgniteProductVersion> minMaxVerPair = ring.minMaxNodeVersions();

        Set<IgniteProductVersion> vers = new HashSet<>();

        vers.add(minMaxVerPair.get1());

        vers.add(minMaxVerPair.get2());

        if (vers.size() > 1)
            throw new IgniteCheckedException("Can't disable rolling upgrade with different versions in cluster: " + vers);

        synchronized (lock) {
            if (lastJoiningNode != null) {
                if (ring.node(lastJoiningNode.id()) != null
                || U.currentTimeMillis() - lastJoiningNodeTimestamp > ((TcpDiscoverySpi)ctx.config().getDiscoverySpi()).getJoinTimeout())
                    lastJoiningNode = null;
            }

            if (lastJoiningNode != null)
                vers.add(IgniteProductVersion.fromString(lastJoiningNode.attribute(ATTR_BUILD_VER)));

            if (vers.size() > 1)
                throw new IgniteCheckedException("Can't disable rolling upgrade with different versions in cluster: " + vers);

            verPair = null;
        }

        metastorage.remove(ROLLING_UPGRADE_VERSIONS_KEY);


        if (log.isInfoEnabled())
            log.info("Rolling upgrade disabled");
    }

    /**
     * Returns a pair containing the current and target versions of the cluster.
     * <p>
     * This method returns {@code null} if rolling upgrade has not been enabled yet
     * or if version information has not been read from the distributed metastorage.
     *
     * @return A pair where:
     *     <ul>
     *         <li><b>First element</b> — current version of the cluster.</li>
     *         <li><b>Second element</b> — target version to which the cluster is being upgraded.</li>
     *     </ul>
     *     or {@code null} if rolling upgrade is not active.
     */
    public IgnitePair<IgniteProductVersion> versions() {
        if (ctx.clientNode()) {
            try {
                return metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }
        return verPair;
    }

    /** Checks whether the cluster is in the rolling upgrade mode. */
    public boolean enabled() {
        return versions() != null;
    }

    /**
     * @param minMaxVersionSupplier Min max versions of nodes in cluster supplier.
     */
    public void ring(TcpDiscoveryNodesRing ring) {
        this.ring = ring;
    }

    /**
     * Checks cur and target versions.
     *
     * @param cur Current cluster version.
     * @param target Target cluster version.
     * @return {@code false} if there is no need to update versions {@code true} otherwise.
     * @throws IgniteCheckedException If versions are incorrect.
     */
    private boolean checkVersionsForEnabling(IgniteProductVersion cur, IgniteProductVersion target) throws IgniteCheckedException {
        if (cur.major() != target.major()) {
            String errMsg = "Major versions are different.";

            log.warning(errMsg);

            throw new IgniteCheckedException(errMsg);
        }

        if (cur.minor() != target.minor()) {
            if (target.minor() == cur.minor() + 1 && target.maintenance() == 0)
                return true;

            String errMsg = "Minor version can only be incremented by 1.";

            log.warning(errMsg);

            throw new IgniteCheckedException(errMsg);
        }

        if (cur.maintenance() + 1 != target.maintenance()) {
            String errMsg = "Patch version can only be incremented by 1.";

            log.warning(errMsg);

            throw new IgniteCheckedException(errMsg);
        }

        return true;
    }

    /** Checks if versions have same major, minor and maintenance versions. */
    private boolean validateVersionsForJoining(IgniteProductVersion ver1, IgniteProductVersion ver2) {
        if (ver1 == null || ver2 == null)
            return false;

        return ver1.major() == ver2.major() && ver1.minor() == ver2.minor() && ver1.maintenance() == ver2.maintenance();
    }
}

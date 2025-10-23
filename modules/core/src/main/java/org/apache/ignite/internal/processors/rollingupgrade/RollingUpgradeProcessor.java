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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNodesRing;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX;

/** Rolling upgrade processor. Manages current and target versions of cluster. */
public class RollingUpgradeProcessor extends GridProcessorAdapter {
    /** Key for the distributed property that holds current and target versions. */
    private static final String ROLLING_UPGRADE_VERSIONS_KEY = IGNITE_INTERNAL_KEY_PREFIX + "rolling.upgrade.versions";

    /** Joining timeout. */
    private static final long JOINING_TIMEOUT = 2000;

    /** Metastorage with the write access. */
    @Nullable private volatile DistributedMetaStorage metastorage;

    /** */
    private TcpDiscoveryNodesRing ring;

    /**
     * @param ctx Context.
     */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** Rolling upgrade disabled flag. */
    private volatile boolean disabled;

    /** Last joining node. */
    private volatile ClusterNode lastJoiningNode = null;

    /** Last joining node timestamp. */
    private volatile long lastJoiningNodeTimestamp;

    /** */
    private final Object lock = new Object();

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                RollingUpgradeProcessor.this.metastorage = metastorage;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        synchronized (lock) {
            lastJoiningNode = node;

            lastJoiningNodeTimestamp = U.currentTimeMillis();

            IgniteProductVersion ringMinVer = ring.minimumNodeVersion();

            if (disabled && !ringMinVer.equals(node.version()))
                return new IgniteNodeValidationResult(node.id(), "Rolling upgrade is disabled. " +
                        "Allowed version for joining: " + ringMinVer.major() + "." + ringMinVer.minor() + "." + ringMinVer.maintenance());
        }
        return null;
    }

    /**
     * Enables rolling upgrade with specified target version.
     *
     * @param target Target version.
     * @throws IgniteCheckedException If versions are incorrect or metastorage is not available.
     */
    public void enable(IgniteProductVersion target) throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        String curBuildVer = ctx.discovery().localNode().attribute(ATTR_BUILD_VER);
        IgniteProductVersion curVer = IgniteProductVersion.fromString(curBuildVer);

        if (!checkVersions(curVer, target))
            return;

        IgnitePair<IgniteProductVersion> newPair = F.pair(curVer, target);

        synchronized (lock) {
            if (!metastorage.compareAndSet(ROLLING_UPGRADE_VERSIONS_KEY, null, newPair))
                throw new IgniteCheckedException("Rolling upgrade is already enabled with a different target version: "
                    + metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY));

            disabled = false;
        }

        log.info("Rolling upgrade enabled [current=" + curVer + ", target=" + target + ']');
    }

    /**
     * Disables rolling upgrade.
     *
     * @throws IgniteCheckedException If metastorage is not available.
     */
    public void disable() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        synchronized (lock) {
            if (disabled) {
                log.info("Rolling upgrade is already disabled");
                return;
            }

            if (lastJoiningNode != null && U.currentTimeMillis() - lastJoiningNodeTimestamp > JOINING_TIMEOUT)
                lastJoiningNode = null;

            Set<IgniteProductVersion> vers = new HashSet<>();

            vers.add(ring.maximumNodeVersion());

            vers.add(ring.minimumNodeVersion());

            if (lastJoiningNode != null)
                vers.add(lastJoiningNode.version());

            if (vers.size() > 1) {
                String msg = "Can't disable rolling upgrade with different versions: " + vers;
                log.warning(msg);
                throw new IgniteCheckedException(msg);
            }

            disabled = true;

            metastorage.removeAsync(ROLLING_UPGRADE_VERSIONS_KEY);

            if (log.isInfoEnabled())
                log.info("Rolling upgrade disabled");
        }
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
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        try {
            return metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Сhecks whether the cluster is in the rolling upgrade mode. */
    public boolean enabled() {
        return versions() != null;
    }

    /**
     * Checks cur and target versions.
     *
     * @param cur Current cluster version.
     * @param target Target cluster version.
     * @return {@code false} if there is no need to update versions {@code true} otherwise.
     * @throws IgniteCheckedException If versions are incorrect.
     */
    private boolean checkVersions(IgniteProductVersion cur, IgniteProductVersion target) throws IgniteCheckedException {
        IgniteProductVersion prevVerPair = metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);

        if (prevVerPair != null) {
            if (prevVerPair.equals(F.pair(cur, target))) {
                if (log.isInfoEnabled())
                    log.info("Rolling upgrade is already enabled with the same current and target versions: " + cur + " , " + target);

                return false;
            }

            String errMsg = "Rolling upgrade is already enabled with a different target version: "
                + metastorage.read(ROLLING_UPGRADE_VERSIONS_KEY);

            log.warning(errMsg);

            throw new IgniteCheckedException(errMsg);
        }

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
}

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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;

/** Rolling upgrade processor. Manages current and target versions of cluster. */
public class RollingUpgradeProcessor extends GridProcessorAdapter {
    /** Key for the distributed property that holds current and target versions. */
    private static final String ROLL_UP_VERSIONS = "rolling.upgrade.versions";

    /** Metastorage with the write access. */
    @Nullable private volatile DistributedMetaStorage metastorage;

    /** Distributed property that holds current and target version. */
    private final AtomicReference<IgnitePair<IgniteProductVersion>> verPairHolder = new AtomicReference<>();

    /**
     * @param ctx Context.
     */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
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

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                RollingUpgradeProcessor.this.metastorage = metastorage;
            }

        });
    }

    /**
     * @param current Current version.
     * @param target Target version.
     */
    public void versions(@NotNull IgniteProductVersion current, @NotNull IgniteProductVersion target) throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (checkVersions(current, target))
            metastorage.write(ROLL_UP_VERSIONS, F.pair(current, target));
    }

    /**
     * Returns a pair containing the current and target versions of the cluster.
     *
     * @return A pair where:
     *     <ul>
     *         <li><b>First element</b> — current version of the cluster.</li>
     *         <li><b>Second element</b> — target version to which the cluster is being upgraded.</li>
     *     </ul>
     */
    public IgnitePair<IgniteProductVersion> versions() {
        IgnitePair<IgniteProductVersion> pair = verPairHolder.get();

        if (pair != null)
            return pair;

        String locBuildVer = ctx.discovery().localNode().attribute(ATTR_BUILD_VER);

        IgniteProductVersion locVer = IgniteProductVersion.fromString(locBuildVer);

        return F.pair(locVer, locVer);
    }

    /** Сhecks whether the cluster is in the rolling upgrade mode. */
    public boolean isRollingUpgradeEnabled() {
        IgnitePair<IgniteProductVersion> pair = verPairHolder.get();
        if (pair != null)
            return !Objects.equals(pair.get1(), pair.get2());

        return false;
    }

    /**
     * @param current Current cluster version.
     * @param target Target cluster version.
     */
    private boolean checkVersions(IgniteProductVersion current, IgniteProductVersion target) {
        assert current != null && target != null;

        return target.compareTo(current) >= 0;
    }
}

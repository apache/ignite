/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.ru;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Provides useful methods related to rolling upgrade.
 */
public abstract class RollingUpgradeUtil {
    /**
     * Returns a list of alive nodes in the cluster that are not updated yet.
     *
     * @param ctx Kernal context.
     * @param status Rolling upgrade status.
     * @return List of alive nodes in the cluster that are not updated yet.
     */
    public static List<String> initialNodes(GridKernalContext ctx, RollingUpgradeStatus status) {
        return nodes(ctx, status, status.initialVersion());
    }

    /**
     * Returns a list of alive nodes in the cluster that are updated.
     *
     * @param ctx Kernal context.
     * @param status Rolling upgrade status.
     * @return List of alive nodes in the cluster that are updated.
     */
    public static List<String> updatedNodes(GridKernalContext ctx, RollingUpgradeStatus status) {
        return nodes(ctx, status, status.targetVersion());
    }

    /**
     * Returns a list of nodes with the given version.
     *
     * @param ctx Kernal context.
     * @param status Rolling upgrade satus.
     * @param ver Ignite product version.
     * @return List of nodes with the given version.
     */
    private static List<String> nodes(GridKernalContext ctx, RollingUpgradeStatus status, IgniteProductVersion ver) {
        if (!status.enabled() || ver == null)
            return Collections.emptyList();

        final DiscoCache disco = ctx.discovery().discoCache();

        return Optional.ofNullable(disco)
            .map(d -> d.allNodes().stream())
            .orElse(Stream.empty())
            .filter(n -> n.version().compareToIgnoreTimestamp(ver) == 0)
            .map(n -> U.id8(n.id()))
            .collect(Collectors.toList());
    }
}

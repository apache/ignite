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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Class represents a transaction test case for single or two keys.
 */
public class TransactionTestCase {
    /** Keys array described with pairs (primaryNodeIdx, backupNodeIdx). */
    @GridToStringInclude
    final Integer[][] keys;

    /** */
    private TransactionTestCase(Integer[][] keys) {
        this.keys = keys;
    }

    /**
     * Builds test cases for checking incremental snapshots.
     *
     * @param nodesCnt  Count of nodes that participated in a test case.
     * @param withBakup If {@code false} then no backups.
     * @return List of test cases.
     */
    public static List<TransactionTestCase> buildTestCases(int nodesCnt, boolean withBakup) {
        List<TransactionTestCase> cases = new ArrayList<>();

        // One entry.
        int backups = withBakup ? nodesCnt : 0;

        for (int primaryNodeIdx = 0; primaryNodeIdx < nodesCnt; primaryNodeIdx++) {
            for (int backupNodeIdx = 0; backupNodeIdx <= backups; backupNodeIdx++) {
                if (withBakup && (backupNodeIdx == primaryNodeIdx || backupNodeIdx == nodesCnt))
                    continue;

                cases.add(new TransactionTestCase(new Integer[][] {new Integer[] {primaryNodeIdx, withBakup ? backupNodeIdx : null}}));
            }
        }

        // Two entries.
        int casesSize = cases.size();

        for (int p1 = 0; p1 < casesSize; p1++) {
            for (int p2 = 0; p2 < casesSize; p2++)
                cases.add(new TransactionTestCase(new Integer[][] {cases.get(p1).keys[0], cases.get(p2).keys[0]}));
        }

        return cases;
    }

    /** */
    public int[] keys(IgniteEx grid, String cache) {
        int[] k = new int[keys.length];

        for (int i = 0; i < keys.length; i++)
            k[i] = key(grid, cache, keys[i][0], keys[i][1]);

        return k;
    }

    /**
     * Provides a key that for an existing partitioning schema match specified primary and backup nodes.
     *
     * @param grid   Ignite grid.
     * @param cache  Cache name.
     * @param primaryNodeIdx Primary node ID.
     * @param backupNodeIdx Backup node ID if specified.
     * @return Key that matches specified primaryNodeIdx and backupNodeIdx nodes.
     */
    static int key(IgniteEx grid, String cache, int primaryNodeIdx, @Nullable Integer backupNodeIdx) {
        List<ClusterNode> nodes = grid.context().discovery().serverNodes(AffinityTopologyVersion.NONE);

        ClusterNode primaryNode = nodes.get(primaryNodeIdx);
        ClusterNode backupNode = backupNodeIdx == null ? null : nodes.get(backupNodeIdx);

        Affinity<Integer> aff = grid.affinity(cache);

        int key = ThreadLocalRandom.current().nextInt();

        while (true) {
            if (aff.isPrimary(primaryNode, key) && (backupNodeIdx == null || aff.isBackup(backupNode, key)))
                return key;

            key++;
        }
    }

    /** */
    public boolean allPrimaryOnNear(int nearNodeIdx) {
        // If all primary partitions are on the near node.
        return Arrays.stream(keys)
            .map(arr -> arr[0])
            .allMatch(primNodeIdx -> primNodeIdx == nearNodeIdx);
    }

    /** */
    public boolean onePhase() {
        int prims = Arrays.stream(keys)
            .map(arr -> arr[0])
            .collect(Collectors.toSet())
            .size();

        int backups = Arrays.stream(keys)
            .map(arr -> arr[1])
            .collect(Collectors.toSet())
            .size();

        return prims == 1 && backups <= 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionTestCase.class, this);
    }
}

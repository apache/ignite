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

package org.apache.ignite.internal.processors.cache.consistentcut;

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

/**
 * Class represents a transaction test case for single or two keys.
 */
public class TransactionTestCase {
    /** Keys array described with pairs (primary, backup). */
    @GridToStringInclude
    private final Integer[][] keys;

    /** */
    private TransactionTestCase(Integer[][] keys) {
        this.keys = keys;
    }

    /**
     * Builds test cases for checking Consistency Cut.
     *
     * @param nodesCnt  Count of nodes that participated in a test case.
     * @param withBakup If {@code false} then no backups.
     * @return List of test cases.
     */
    public static List<TransactionTestCase> buildTestCases(int nodesCnt, boolean withBakup) {
        List<TransactionTestCase> cases = new ArrayList<>();

        // One entry.
        int backups = withBakup ? nodesCnt : 0;

        for (int primary = 0; primary < nodesCnt; primary++) {
            for (int backup = 0; backup <= backups; backup++) {
                if (withBakup && (backup == primary || backup == nodesCnt))
                    continue;

                cases.add(new TransactionTestCase(new Integer[][] {new Integer[] {primary, withBakup ? backup : null}}));
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
    public int firstKeyPrimary() {
        return keys[0][0];
    }

    /** */
    public int firstKeyBackup() {
        return keys[0][1];
    }

    /** */
    public int[] keys(IgniteEx grid, String cache) {
        int[] k = new int[keys.length];

        for (int i = 0; i < keys.length; i++) {
            int primary = keys[i][0];
            Integer backup = keys[i][1];

            List<ClusterNode> nodes = grid.context().discovery().serverNodes(AffinityTopologyVersion.NONE);

            ClusterNode primaryNode = nodes.get(primary);
            ClusterNode backupNode = backup == null ? null : nodes.get(backup);

            Affinity<Integer> aff = grid.affinity(cache);

            int key = ThreadLocalRandom.current().nextInt();

            while (true) {
                if (aff.isPrimary(primaryNode, key) && (backup == null || aff.isBackup(backupNode, key))) {
                    k[i] = key;

                    break;
                }

                key++;
            }
        }

        return k;
    }

    /** */
    public boolean allPrimaryOnNear(int nearNodeId) {
        // If all primary partitions are on the near node.
        return Arrays.stream(keys)
            .map(arr -> arr[0])
            .allMatch(prim -> prim == nearNodeId);
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

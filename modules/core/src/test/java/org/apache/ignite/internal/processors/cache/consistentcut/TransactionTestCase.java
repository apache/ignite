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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Class represents a transaction test case for single or two keys.
 */
public class TransactionTestCase {
    /** Keys array described with pairs (primary, backup). */
    @GridToStringInclude
    private final T2<Integer, Integer>[] keys;

    /** */
    private TransactionTestCase(T2<Integer, Integer>[] keys) {
        this.keys = keys;
    }

    /** Test case with single key. */
    private static TransactionTestCase forSignleKey(Integer primary, @Nullable Integer backup) {
        return new TransactionTestCase(new T2[] {new T2<>(primary, backup)});
    }

    /** Test case with two keys. */
    private static TransactionTestCase forTwoKeys(TransactionTestCase firstKey, TransactionTestCase secondKey) {
        return new TransactionTestCase(new T2[] {firstKey.keys[0], secondKey.keys[0]});
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

                cases.add(forSignleKey(primary, withBakup ? backup : null));
            }
        }

        // Two entries.
        int casesSize = cases.size();

        for (int p1 = 0; p1 < casesSize; p1++) {
            for (int p2 = 0; p2 < casesSize; p2++)
                cases.add(forTwoKeys(cases.get(p1), cases.get(p2)));
        }

        return cases;
    }

    /** */
    public int firstKeyPrimary() {
        return keys[0].getKey();
    }

    /** */
    public int firstKeyBackup() {
        return keys[0].getValue();
    }

    /** */
    public int[] keys(IgniteEx grid, String cache) {
        int[] k = new int[keys.length];

        for (int i = 0; i < keys.length; i++)
            k[i] = key(grid, cache, i);

        return k;
    }

    /**
     * Provides a key that for an existing partitioning schema match specified primary and backup node.
     *
     * @param grid   Ignite grid.
     * @param cache  Cache name.
     * @param keyNum Number of key in the test case.
     * @return Key that matches specified primary and backup nodes.
     */
    private int key(IgniteEx grid, String cache, int keyNum) {
        int primary = keys[keyNum].getKey();
        Integer backup = keys[keyNum].getValue();

        List<ClusterNode> nodes = grid.context().discovery().serverNodes(AffinityTopologyVersion.NONE);

        ClusterNode primaryNode = nodes.get(primary);
        ClusterNode backupNode = backup == null ? null : nodes.get(backup);

        Affinity<Integer> aff = grid.affinity(cache);

        int key = ThreadLocalRandom.current().nextInt();

        while (true) {
            if (aff.isPrimary(primaryNode, key) && (backup == null || aff.isBackup(backupNode, key)))
                return key;

            key++;
        }
    }

    /** */
    public boolean allPrimaryOnNear(int nearNodeId) {
        // If all primary partitions are on the near node.
        return Arrays.stream(keys)
            .map(IgniteBiTuple::get1)
            .allMatch(prim -> prim == nearNodeId);
    }

    /** */
    public boolean onePhase() {
        int prims = Arrays.stream(keys)
            .map(T2::get1)
            .collect(Collectors.toSet())
            .size();

        int backups = Arrays.stream(keys)
            .map(T2::get2)
            .collect(Collectors.toSet())
            .size();

        return prims == 1 && backups <= 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionTestCase.class, this);
    }
}

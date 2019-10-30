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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PartitionsDistribution {
    public static final int[] ALL_PARTS = new int[0];
    private static final int[] EMPTY = new int[0];

    public boolean excessive;
    public int parts;
    public int[] nodes;
    public int[][] nodeParts;

    public PartitionsDistribution mergeWith(PartitionsDistribution other) {
        if (parts != 0 && other.parts != 0 && parts != other.parts)
            throw new IllegalStateException("Non-collocated query fragment.");

        int[] nodes0 = null;
        int[][] nodeParts0 = null;

        int i = 0, j = 0, k = 0;

        while (i < nodes.length && j < other.nodes.length) {
            if (nodes[i] < other.nodes[j])
                i++;
            else if (other.nodes[j] < nodes[i])
                j++;
            else {
                int[] mergedParts = merge(nodeParts[i], other.nodeParts[j]);

                if (mergedParts.length > 0) {
                    if (nodes0 == null) {
                        int len = Math.min(nodes.length, other.nodes.length);

                        nodes0 = new int[len];
                        nodeParts0 = new int[len][];
                    }

                    nodes0[k] = nodes[i];
                    nodeParts0[k] = mergedParts;

                    k++;
                }

                i++;
                j++;
            }
        }

        PartitionsDistribution res = new PartitionsDistribution();

        res.excessive = excessive && other.excessive;
        res.parts = Math.max(parts, other.parts);
        res.nodes = nodes0.length == k ? nodes0 : Arrays.copyOf(nodes0, k);
        res.nodeParts = nodeParts0.length == k ? nodeParts0 : Arrays.copyOf(nodeParts0, k);

        check(res);

        return res;
    }

    private void check(PartitionsDistribution res) {
        if (res.parts == 0)
            return; // Only receivers and/or replicated caches in task subtree.

        BitSet check = new BitSet(res.parts);

        int checkedParts = 0;

        for (int[] nodePart : res.nodeParts) {
            for (int p : nodePart) {
                if (!check.get(p)) {
                    check.set(p);
                    checkedParts++;
                }
            }
        }

        if (checkedParts < res.parts)
            throw new IllegalStateException("Failed to collocate used caches.");
    }

    private int[] merge(int[] left, int[] right) {
        if (left == ALL_PARTS)
            return right;
        if (right == ALL_PARTS)
            return left;

        int[] nodeParts = null;

        int i = 0, j = 0, k = 0;

        while (i < left.length && j < right.length) {
            if (left[i] < right[j])
                i++;
            else if (right[j] < left[i])
                j++;
            else {
                if (nodeParts == null)
                    nodeParts = new int[Math.min(left.length, right.length)];

                nodeParts[k++] = left[i];

                i++;
                j++;
            }
        }

        if (k == 0)
            return EMPTY;

        return nodeParts.length == k ? nodeParts : Arrays.copyOf(nodeParts, k);
    }

    public PartitionsDistribution deduplicate() {
        if (!excessive)
            return this;

        Map<Integer, Integer> map = new HashMap<>();

        int idx = 0; int[] idxs = new int[nodeParts.length];
        while (map.size() < parts) {
            int[] nodePart = nodeParts[idx];

            int j = idxs[idx];

            while (j < nodePart.length) {
                if (map.putIfAbsent(nodePart[j], nodes[idx]) == null || j + 1 == nodePart.length) {
                    idxs[idx] = j + 1;

                    break;
                }

                j++;
            }

            idx = (idx + 1) % nodes.length;
        }

        int[] nodes0 = new int[nodes.length]; int[][] nodeParts0 = new int[nodes.length][];

        int k = 0;

        for (int i = 0; i < nodes.length; i++) {
            int j = 0;

            int[] nodePart0 = null;

            for (int p : nodeParts[i]) {
                if (map.get(p) == nodes[i]) {
                    if (nodePart0 == null)
                        nodePart0 = new int[nodeParts[i].length];

                    nodePart0[j++] = p;
                }
            }

            if (nodePart0 != null) {
                nodes0[k] = nodes[i];
                nodeParts0[k] = nodePart0.length == j ? nodePart0 : Arrays.copyOf(nodePart0, j);

                k++;
            }
        }

        PartitionsDistribution res = new PartitionsDistribution();

        res.parts = parts;
        res.nodes = nodes0.length == k ? nodes0 : Arrays.copyOf(nodes0, k);
        res.nodeParts = nodeParts0.length == k ? nodeParts0 : Arrays.copyOf(nodeParts0, k);

        return res;
    }
}

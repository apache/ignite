/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.tree.leaf;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * Decision tree leaf node builder that chooses most common value as a leaf node value.
 */
public class MostCommonDecisionTreeLeafBuilder implements DecisionTreeLeafBuilder {
    /** {@inheritDoc} */
    @Override public DecisionTreeLeafNode createLeafNode(Dataset<EmptyContext, DecisionTreeData> dataset,
        TreeFilter pred) {
        Map<Double, Integer> cnt = dataset.compute(part -> {

            if (part.getFeatures() != null) {
                Map<Double, Integer> map = new HashMap<>();

                for (int i = 0; i < part.getFeatures().length; i++) {
                    if (pred.test(part.getFeatures()[i])) {
                        double lb = part.getLabels()[i];

                        if (map.containsKey(lb))
                            map.put(lb, map.get(lb) + 1);
                        else
                            map.put(lb, 1);
                    }
                }

                return map;
            }

            return null;
        }, this::reduce);

        double bestVal = 0;
        int bestCnt = -1;

        for (Map.Entry<Double, Integer> e : cnt.entrySet()) {
            if (e.getValue() > bestCnt) {
                bestCnt = e.getValue();
                bestVal = e.getKey();
            }
        }

        return new DecisionTreeLeafNode(bestVal);
    }

    /** */
    private Map<Double, Integer> reduce(Map<Double, Integer> a, Map<Double, Integer> b) {
        if (a == null)
            return b;
        else if (b == null)
            return a;
        else {
            for (Map.Entry<Double, Integer> e : b.entrySet()) {
                if (a.containsKey(e.getKey()))
                    a.put(e.getKey(), a.get(e.getKey()) + e.getValue());
                else
                    a.put(e.getKey(), e.getValue());
            }
            return a;
        }
    }
}

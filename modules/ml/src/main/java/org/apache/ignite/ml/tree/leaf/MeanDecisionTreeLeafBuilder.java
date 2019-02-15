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

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * Decision tree leaf node builder that chooses mean value as a leaf value.
 */
public class MeanDecisionTreeLeafBuilder implements DecisionTreeLeafBuilder {
    /** {@inheritDoc} */
    @Override public DecisionTreeLeafNode createLeafNode(Dataset<EmptyContext, DecisionTreeData> dataset,
        TreeFilter pred) {
        double[] aa = dataset.compute(part -> {
            double mean = 0;
            int cnt = 0;

            for (int i = 0; i < part.getFeatures().length; i++) {
                if (pred.test(part.getFeatures()[i])) {
                    mean += part.getLabels()[i];
                    cnt++;
                }
            }

            if (cnt != 0) {
                mean = mean / cnt;

                return new double[] {mean, cnt};
            }

            return null;
        }, this::reduce);

        return aa != null ? new DecisionTreeLeafNode(aa[0]) : null;
    }

    /** */
    private double[] reduce(double[] a, double[] b) {
        if (a == null)
            return b;
        else if (b == null)
            return a;
        else {
            double aMean = a[0];
            double aCnt = a[1];
            double bMean = b[0];
            double bCnt = b[1];

            double mean = (aMean * aCnt + bMean * bCnt) / (aCnt + bCnt);

            return new double[] {mean, aCnt + bCnt};
        }
    }
}

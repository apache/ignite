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

package org.apache.ignite.ml.tree.impurity.gini;

import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 * Gini impurity measure which is calculated the following way:
 * {@code \-frac{1}{L}\sum_{i=1}^{s}l_i^2 - \frac{1}{R}\sum_{i=s+1}^{n}r_i^2}.
 */
public class GiniImpurityMeasure implements ImpurityMeasure<GiniImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 5338129703395229970L;

    /** Number of elements of each type in the left part. */
    private final long[] left;

    /** Number of elements of each type in the right part. */
    private final long[] right;

    /**
     * Constructs a new instance of Gini impurity measure.
     *
     * @param left Number of elements of each type in the left part.
     * @param right Number of elements of each type in the right part.
     */
    GiniImpurityMeasure(long[] left, long[] right) {
        assert left.length == right.length : "Left and right parts have to be the same length";

        this.left = left;
        this.right = right;
    }

    /** {@inheritDoc} */
    @Override public double impurity() {
        long leftCnt = 0;
        long rightCnt = 0;

        double leftImpurity = 0;
        double rightImpurity = 0;

        for (long e : left)
            leftCnt += e;

        for (long e : right)
            rightCnt += e;

        if (leftCnt > 0)
            for (long e : left)
                leftImpurity += Math.pow(e, 2) / leftCnt;

        if (rightCnt > 0)
            for (long e : right)
                rightImpurity += Math.pow(e, 2) / rightCnt;

        return -(leftImpurity + rightImpurity);
    }

    /** {@inheritDoc} */
    @Override public GiniImpurityMeasure add(GiniImpurityMeasure b) {
        assert left.length == b.left.length : "Subtracted measure has to have length " + left.length;
        assert left.length == b.right.length : "Subtracted measure has to have length " + left.length;

        long[] leftRes = new long[left.length];
        long[] rightRes = new long[left.length];

        for (int i = 0; i < left.length; i++) {
            leftRes[i] = left[i] + b.left[i];
            rightRes[i] = right[i] + b.right[i];
        }

        return new GiniImpurityMeasure(leftRes, rightRes);
    }

    /** {@inheritDoc} */
    @Override public GiniImpurityMeasure subtract(GiniImpurityMeasure b) {
        assert left.length == b.left.length : "Subtracted measure has to have length " + left.length;
        assert left.length == b.right.length : "Subtracted measure has to have length " + left.length;

        long[] leftRes = new long[left.length];
        long[] rightRes = new long[left.length];

        for (int i = 0; i < left.length; i++) {
            leftRes[i] = left[i] - b.left[i];
            rightRes[i] = right[i] - b.right[i];
        }

        return new GiniImpurityMeasure(leftRes, rightRes);
    }

    /** */
    public long[] getLeft() {
        return left;
    }

    /** */
    public long[] getRight() {
        return right;
    }
}

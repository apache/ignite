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

package org.apache.ignite.ml.tree.impurity.mse;

import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 * Mean squared error (variance) impurity measure which is calculated the following way:
 * {@code \frac{1}{L}\sum_{i=0}^{n}(y_i - \mu)^2}.
 */
public class MSEImpurityMeasure implements ImpurityMeasure<MSEImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 4536394578628409689L;

    /** Sum of all elements in the left part. */
    private final double leftY;

    /** Sum of all squared elements in the left part. */
    private final double leftY2;

    /** Number of elements in the left part. */
    private final long leftCnt;

    /** Sum of all elements in the right part. */
    private final double rightY;

    /** Sum of all squared elements in the right part. */
    private final double rightY2;

    /** Number of elements in the right part. */
    private final long rightCnt;

    /**
     * Constructs a new instance of mean squared error (variance) impurity measure.
     *
     * @param leftY Sum of all elements in the left part.
     * @param leftY2 Sum of all squared elements in the left part.
     * @param leftCnt Number of elements in the left part.
     * @param rightY Sum of all elements in the right part.
     * @param rightY2 Sum of all squared elements in the right part.
     * @param rightCnt Number of elements in the right part.
     */
    public MSEImpurityMeasure(double leftY, double leftY2, long leftCnt, double rightY, double rightY2, long rightCnt) {
        this.leftY = leftY;
        this.leftY2 = leftY2;
        this.leftCnt = leftCnt;
        this.rightY = rightY;
        this.rightY2 = rightY2;
        this.rightCnt = rightCnt;
    }

    /** {@inheritDoc} */
    @Override public double impurity() {
        double impurity = 0;

        if (leftCnt > 0)
            impurity += leftY2 - 2.0 * leftY / leftCnt * leftY + Math.pow(leftY / leftCnt, 2) * leftCnt;

        if (rightCnt > 0)
            impurity += rightY2 - 2.0 * rightY / rightCnt * rightY + Math.pow(rightY / rightCnt, 2) * rightCnt;

        return impurity;
    }

    /** {@inheritDoc} */
    @Override public MSEImpurityMeasure add(MSEImpurityMeasure b) {
        return new MSEImpurityMeasure(
            leftY + b.leftY,
            leftY2 + b.leftY2,
            leftCnt + b.leftCnt,
            rightY + b.rightY,
            rightY2 + b.rightY2,
            rightCnt + b.rightCnt
        );
    }

    /** {@inheritDoc} */
    @Override public MSEImpurityMeasure subtract(MSEImpurityMeasure b) {
        return new MSEImpurityMeasure(
            leftY - b.leftY,
            leftY2 - b.leftY2,
            leftCnt - b.leftCnt,
            rightY - b.rightY,
            rightY2 - b.rightY2,
            rightCnt - b.rightCnt
        );
    }

    /** */
    public double getLeftY() {
        return leftY;
    }

    /** */
    public double getLeftY2() {
        return leftY2;
    }

    /** */
    public long getLeftCnt() {
        return leftCnt;
    }

    /** */
    public double getRightY() {
        return rightY;
    }

    /** */
    public double getRightY2() {
        return rightY2;
    }

    /** */
    public long getRightCnt() {
        return rightCnt;
    }
}

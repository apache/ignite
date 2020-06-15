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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.BitSet;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** */
public class RightJoinNode<Row> extends AbstractJoinNode<Row> {
    /** Right row factory. */
    private final RowHandler.RowFactory<Row> leftRowFactory;

    /** */
    private BitSet rightNotMatchedIndexes;

    /** */
    private int lastPushedInd;

    /** */
    private Row left;

    /** */
    private int rightIdx;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public RightJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> leftRowFactory) {
        super(ctx, cond);

        this.leftRowFactory = leftRowFactory;
    }

    /** {@inheritDoc} */
    @Override protected void doJoinInternal() {
        if (waitingRight == NOT_WAITING) {
            if (rightNotMatchedIndexes == null) {
                rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                rightNotMatchedIndexes.set(0, rightMaterialized.size());
            }

            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null)
                        left = leftInBuf.remove();

                    while (requested > 0 && rightIdx < rightMaterialized.size()) {
                        Row right = rightMaterialized.get(rightIdx++);
                        Row joined = handler.concat(left, right);

                        if (!cond.test(joined))
                            continue;

                        requested--;
                        rightNotMatchedIndexes.clear(rightIdx - 1);
                        downstream.push(joined);
                    }

                    if (rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }
            finally {
                inLoop = false;
            }
        }

        if (waitingLeft == NOT_WAITING && requested > 0 && !rightNotMatchedIndexes.isEmpty()) {
            assert lastPushedInd >= 0;

            inLoop = true;
            try {
                for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd);;
                    lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                ) {
                    if (lastPushedInd < 0)
                        break;

                    Row row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                    rightNotMatchedIndexes.clear(lastPushedInd);

                    requested--;
                    downstream.push(row);

                    if (lastPushedInd == Integer.MAX_VALUE || requested <= 0)
                        break;
                }
            }
            finally {
                inLoop = false;
            }
        }

        if (waitingRight == 0)
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

        if (waitingLeft == 0 && leftInBuf.isEmpty())
            sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
            && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
            downstream.end();
            requested = 0;
        }
    }
}

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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** */
public class FullOuterJoinNode<Row> extends AbstractJoinNode<Row> {
    /** Left row factory. */
    private final RowHandler.RowFactory<Row> leftRowFactory;

    /** Right row factory. */
    private final RowHandler.RowFactory<Row> rightRowFactory;

    /** Whether current left row was matched or not. */
    private boolean leftMatched;

    /** */
    private final Set<Row> rightNotMatched = new HashSet<>();

    /** */
    private Row left;

    /** */
    private int rightIdx;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public FullOuterJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond, RowHandler.RowFactory<Row> leftRowFactory,
        RowHandler.RowFactory<Row> rightRowFactory) {
        super(ctx, cond);

        this.leftRowFactory = leftRowFactory;
        this.rightRowFactory = rightRowFactory;
    }

    /** {@inheritDoc} */
    @Override protected void pushRight(Row row) {
        super.pushRight(row);

        rightNotMatched.add(row);
    }

    /** {@inheritDoc} */
    @Override protected void doJoin() {
        if (waitingRight == -1) {
            while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                if (left == null) {
                    left = leftInBuf.remove();

                    leftMatched = false;
                }

                while (requested > 0 && rightIdx < rightMaterialized.size()) {
                    Row right = rightMaterialized.get(rightIdx++);
                    Row joined = handler.concat(left, right);

                    if (!cond.test(joined))
                        continue;

                    requested--;
                    leftMatched = true;
                    rightNotMatched.remove(right);
                    downstream.push(joined);
                }

                if (rightIdx == rightMaterialized.size()) {
                    boolean wasPushed = false;

                    if (!leftMatched && requested > 0) {
                        requested--;
                        wasPushed = true;

                        downstream.push(handler.concat(left, rightRowFactory.create()));
                    }

                    if (leftMatched || wasPushed) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }
        }

        if (waitingLeft == -1 && requested > 0 && !rightNotMatched.isEmpty()) {
            Iterator<Row> it = rightNotMatched.iterator();

            while (it.hasNext() && requested > 0) {
                Row row = handler.concat(leftRowFactory.create(), it.next());

                it.remove();

                requested--;
                downstream.push(row);
            }
        }

        if (waitingRight == 0)
            sources.get(1).request(waitingRight = IN_BUFFER_SIZE);

        if (waitingLeft == 0 && leftInBuf.isEmpty())
            sources.get(0).request(waitingLeft = IN_BUFFER_SIZE);

        if (requested > 0 && waitingLeft == -1 && waitingRight == -1 && left == null && leftInBuf.isEmpty() && rightNotMatched.isEmpty()) {
            downstream.end();
            requested = 0;
        }
    }
}

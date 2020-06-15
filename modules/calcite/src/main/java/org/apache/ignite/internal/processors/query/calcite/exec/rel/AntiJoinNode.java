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

import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/** */
public class AntiJoinNode<Row> extends AbstractJoinNode<Row> {
    /** */
    private Row left;

    /** */
    private int rightIdx;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    public AntiJoinNode(ExecutionContext<Row> ctx, Predicate<Row> cond) {
        super(ctx, cond);
    }

    /** {@inheritDoc} */
    @Override protected void doJoinInternal() {
        if (waitingRight == NOT_WAITING) {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null)
                        left = leftInBuf.remove();

                    boolean matched = false;

                    while (!matched && rightIdx < rightMaterialized.size()) {
                        Row row = handler.concat(left, rightMaterialized.get(rightIdx++));

                        if (cond.test(row))
                            matched = true;
                    }

                    if (!matched) {
                        requested--;
                        downstream.push(left);
                    }

                    left = null;
                    rightIdx = 0;
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

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
            downstream.end();
            requested = 0;
        }
    }
}

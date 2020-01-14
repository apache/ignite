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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.function.BiFunction;

/**
 * TODO remove buffers.
 */
public class JoinNode extends AbstractNode<Object[]> {
    /** */
    private final BiFunction<Object[], Object[], Object[]> expression;

    /** */
    private final ArraySink<Object[]> left;

    /** */
    private final ArraySink<Object[]> right;

    /** */
    private int leftIdx;

    /** */
    private int rightIdx;

    /** */
    private boolean end;

    /**
     * @param ctx Execution context.
     * @param expression Join expression.
     */
    public JoinNode(ExecutionContext ctx, Node<Object[]> left, Node<Object[]> right, BiFunction<Object[], Object[], Object[]> expression) {
        super(ctx, ImmutableList.of(left, right));

        this.expression = expression;
        this.left = new ArraySink<>();
        this.right = new ArraySink<>();

        link();
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        switch (idx) {
            case 0:
                return left;
            case 1:
                return right;
            default:
                throw new IndexOutOfBoundsException();
        }
    }

    /** {@inheritDoc} */
    @Override public void request() {
        checkThread();

        if (context().cancelled() || end)
            return;

        if (!left.end)
            input(0).request();
        if (!right.end)
            input(1).request();
        if (left.end && right.end)
            tryFlush();
    }

    /** */
    public void tryFlush() {
        if (left.end && right.end) {
            for (int i = leftIdx; i < left.size(); i++) {
                for (int j = rightIdx; j < right.size(); j++) {
                    if (context().cancelled())
                        return;

                    Object[] row = expression.apply(left.get(i), right.get(j));

                    if (row != null && !target().push(row)) {
                        leftIdx = i;
                        rightIdx = j;

                        return;
                    }
                }
            }

            end = true;
            target().end();
        }
    }

    /** */
    private final class ArraySink<T> extends ArrayList<T> implements Sink<T> {
        /** */
        private boolean end;

        /** {@inheritDoc} */
        @Override public boolean push(T row) {
            return add(row);
        }

        /** {@inheritDoc} */
        @Override public void end() {
            end = true;

            tryFlush();
        }
    }
}

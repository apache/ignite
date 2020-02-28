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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * TODO remove buffers.
 */
public class JoinNode extends AbstractNode<Object[]> {
    /** */
    private final Predicate<Object[]> condition;

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
     * @param condition Join expression.
     */
    public JoinNode(ExecutionContext ctx, Node<Object[]> left, Node<Object[]> right, Predicate<Object[]> condition) {
        super(ctx, ImmutableList.of(left, right));

        this.condition = condition;
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
        if (!end)
            tryFlush();
    }

    /** */
    public void tryFlush() {
        assert !end;

        if (left.end && right.end) {
            for (int i = leftIdx; i < left.size(); i++) {
                for (int j = rightIdx; j < right.size(); j++) {
                    if (context().cancelled())
                        return;

                    Object[] row = F.concat(left.get(i), right.get(j));

                    if (condition.test(row) && !target().push(row)) {
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

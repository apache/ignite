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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.function.BiFunction;

/**
 *
 */
public class JoinNode extends AbstractNode<Object[]> {
    private final BiFunction<Object[], Object[], Object[]> expression;
    private final ArraySink<Object[]> left;
    private final ArraySink<Object[]> right;

    private int leftIdx;
    private int rightIdx;
    private boolean end;

    public JoinNode(Sink<Object[]> target, BiFunction<Object[], Object[], Object[]> expression) {
        super(target);
        this.expression = expression;

        left = new ArraySink<>();
        right = new ArraySink<>();
    }

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

    @Override public void signal() {
        if (end)
            return;

        if (left.end && right.end)
            tryFlush();

        assert sources != null && sources.size() == 2;

        if (!left.end)
            signal(0);
        if (!right.end)
            signal(1);
    }

    public void tryFlush() {
        if (left.end && right.end) {
            for (int i = leftIdx; i < left.size(); i++) {
                for (int j = rightIdx; j < right.size(); j++) {
                    Object[] row = expression.apply(left.get(i), right.get(j));

                    if (row != null && !target.push(row)) {
                        leftIdx = i;
                        rightIdx = j;

                        return;
                    }
                }
            }

            end = true;
            target.end();
        }
    }

    private final class ArraySink<T> extends ArrayList<T> implements Sink<T> {
        private boolean end;

        @Override public boolean push(T row) {
            return add(row);
        }

        @Override public void end() {
            end = true;

            tryFlush();
        }
    }
}

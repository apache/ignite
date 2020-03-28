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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class FilterNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Downstream<Object[]> {
    /** */
    private final Predicate<Object[]> predicate;

    /** */
    private Deque<Object[]> inBuffer = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param predicate Predicate.
     */
    public FilterNode(ExecutionContext ctx, Predicate<Object[]> predicate) {
        super(ctx);

        this.predicate = predicate;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (!inLoop)
            context().execute(this::flushFromBuffer);
    }

    /** {@inheritDoc} */
    @Override public void push(Object[] row) {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting--;

        try {
            if (predicate.test(row))
                inBuffer.add(row);

            flushFromBuffer();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting = -1;

        try {
            flushFromBuffer();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    public void flushFromBuffer() {
        inLoop = true;
        try {
            while (requested > 0 && !inBuffer.isEmpty()) {
                requested--;
                downstream.push(inBuffer.remove());
            }

            if (inBuffer.isEmpty() && waiting == 0)
                F.first(sources).request(waiting = IN_BUFFER_SIZE);

            if (waiting == -1 && requested > 0) {
                assert inBuffer.isEmpty();

                downstream.end();
                requested = 0;
            }
        }
        finally {
            inLoop = false;
        }
    }
}

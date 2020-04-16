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

import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class UnionAllNode<T> extends AbstractNode<T> implements Downstream<T> {
    /** */
    private int curSrc;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public UnionAllNode(ExecutionContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<T> requestDownstream(int idx) {
        assert sources != null;
        assert idx >= 0 && idx < sources.size();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources);
        assert rowsCount > 0 && waiting == 0;

        source().request(waiting = rowsCount);
    }

    /** {@inheritDoc} */
    @Override public void push(T row) {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting--;

        downstream.push(row);
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        if (++curSrc < sources.size())
            source().request(waiting);
        else {
            waiting = -1;
            downstream.end();
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    private Node<T> source() {
        return sources.get(curSrc);
    }
}

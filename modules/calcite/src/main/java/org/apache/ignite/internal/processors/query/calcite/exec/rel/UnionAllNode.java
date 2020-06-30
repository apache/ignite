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
public class UnionAllNode<Row> extends AbstractNode<Row> implements Downstream<Row> {
    /** */
    private int curSrc;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public UnionAllNode(ExecutionContext<Row> ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        assert sources != null;
        assert idx >= 0 && idx < sources.size();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources);
        assert rowsCnt > 0 && waiting == 0;

        try {
            source().request(waiting = rowsCnt);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
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

        try {
            if (++curSrc < sources.size())
                source().request(waiting);
            else {
                waiting = -1;
                downstream.end();
            }
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected void resetInternal() {
        curSrc = 0;
        waiting = 0;
    }

    /** */
    private Node<Row> source() {
        return sources.get(curSrc);
    }
}

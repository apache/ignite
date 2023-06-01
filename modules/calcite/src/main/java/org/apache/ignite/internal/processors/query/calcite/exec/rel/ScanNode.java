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

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Scan node.
 */
public class ScanNode<Row> extends AbstractNode<Row> implements SingleNode<Row> {
    /** */
    private final Iterable<Row> src;

    /** */
    private Iterator<Row> it;

    /** */
    private int requested;

    /** */
    private boolean inLoop;

    /** */
    private boolean firstReq = true;

    /**
     * @param ctx Execution context.
     * @param src Source.
     */
    public ScanNode(ExecutionContext<Row> ctx, RelDataType rowType, Iterable<Row> src) {
        super(ctx, rowType);

        this.src = src;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            if (firstReq) {
                try {
                    push(); // Make first request sync to reduce latency in simple cases.
                }
                catch (Throwable e) {
                    onError(e);
                }

                firstReq = false;
            }
            else
                context().execute(this::push, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override public void closeInternal() {
        super.closeInternal();

        Commons.closeQuiet(it);
        it = null;
        Commons.closeQuiet(src);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        Commons.closeQuiet(it);
        it = null;
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** */
    private void push() throws Exception {
        if (isClosed())
            return;

        checkState();

        inLoop = true;
        try {
            context().ioTracker().startTracking();

            if (it == null)
                it = src.iterator();

            int processed = 0;
            while (requested > 0 && it.hasNext()) {
                checkState();

                requested--;
                downstream().push(it.next());

                if (++processed == IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::push, this::onError);

                    return;
                }
            }
        }
        finally {
            context().ioTracker().stopTracking();

            inLoop = false;
        }

        if (requested > 0 && !it.hasNext()) {
            Commons.closeQuiet(it);
            it = null;

            requested = 0;

            downstream().end();
        }
    }
}

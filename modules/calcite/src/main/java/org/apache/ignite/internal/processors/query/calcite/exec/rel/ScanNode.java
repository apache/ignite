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
public class ScanNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT> {
    /**
     *
     */
    private final Iterable<RowT> src;

    /**
     *
     */
    private Iterator<RowT> it;

    /**
     *
     */
    private int requested;

    /**
     *
     */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param src Source.
     */
    public ScanNode(ExecutionContext<RowT> ctx, RelDataType rowType, Iterable<RowT> src) {
        super(ctx, rowType);

        this.src = src;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::push, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        Commons.closeQuiet(it);
        it = null;
        Commons.closeQuiet(src);
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        Commons.closeQuiet(it);
        it = null;
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     */
    private void push() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        inLoop = true;
        try {
            if (it == null) {
                it = src.iterator();
            }

            int processed = 0;
            while (requested > 0 && it.hasNext()) {
                checkState();

                requested--;
                downstream().push(it.next());

                if (++processed == inBufSize && requested > 0) {
                    // allow others to do their job
                    context().execute(this::push, this::onError);

                    return;
                }
            }
        } finally {
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

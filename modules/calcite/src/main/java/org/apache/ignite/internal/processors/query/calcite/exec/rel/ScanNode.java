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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Scan node.
 */
public class ScanNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, AutoCloseable {
    /** */
    private final Iterable<Object[]> source;

    /** */
    private Iterator<Object[]> it;

    /** */
    private int requested;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param source Source.
     */
    public ScanNode(ExecutionContext ctx, Iterable<Object[]> source) {
        super(ctx);

        this.source = source;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (!inLoop)
            context().execute(this::pushInternal);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();
        context().markCancelled();
        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Commons.closeQuiet(it);
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Object[]>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** */
    private void pushInternal() {
        inLoop = true;
        try {
            if (it == null)
                it = source.iterator();

            Thread thread = Thread.currentThread();

            while (requested > 0 && it.hasNext()) {
                if (context().cancelled())
                    return;

                if (thread.isInterrupted())
                    throw new IgniteInterruptedCheckedException("Thread was interrupted.");

                requested--;
                downstream.push(it.next());
            }

            if (requested > 0 && !it.hasNext()) {
                downstream.end();
                requested = 0;

                close();
            }
        }
        catch (Throwable e) {
            close();

            downstream.onError(e);
        }
        finally {
            inLoop = false;
        }
    }
}

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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CALCITE_EXEC_IO_BATCH_CNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CALCITE_EXEC_IO_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CALCITE_EXEC_MODIFY_BATCH_SIZE;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<Row> implements Node<Row> {
    /** */
    protected static final int IN_BUFFER_SIZE = IgniteSystemProperties.getInteger(IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE, 512);

    /** */
    protected static final int MODIFY_BATCH_SIZE = IgniteSystemProperties.getInteger(IGNITE_CALCITE_EXEC_MODIFY_BATCH_SIZE, 100);

    /** */
    protected static final int IO_BATCH_SIZE = IgniteSystemProperties.getInteger(IGNITE_CALCITE_EXEC_IO_BATCH_SIZE, 256);

    /** */
    protected static final int IO_BATCH_CNT = IgniteSystemProperties.getInteger(IGNITE_CALCITE_EXEC_IO_BATCH_CNT, 4);

    /**
     * {@link Inbox} node may not have proper context at creation time in case it
     * creates on first message received from a remote source. This case the context
     * sets in scope of {@link Inbox#init(ExecutionContext, RelDataType, Collection, Comparator)} method call.
     */
    private ExecutionContext<Row> ctx;

    /** */
    private RelDataType rowType;

    /** */
    private Downstream<Row> downstream;

    /** */
    private boolean closed;

    /** */
    private List<Node<Row>> sources;

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        this.ctx = ctx;
        this.rowType = rowType;
    }

    /**
     * {@link Inbox} node may not have proper context at creation time in case it
     * creates on first message received from a remote source. This case the context
     * sets in scope of {@link Inbox#init(ExecutionContext, Collection, Comparator)} method call.
     */ /** {@inheritDoc} */
    @Override public ExecutionContext<Row> context() {
        return ctx;
    }

    /** */
    protected void context(ExecutionContext<Row> ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType() {
        return rowType;
    }

    /** */
    protected void rowType(RelDataType rowType) {
        this.rowType = rowType;
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        this.sources = sources;

        for (int i = 0; i < sources.size(); i++)
            sources.get(i).onRegister(new DelegateDownstream(requestDownstream(i)));
    }

    /** */
    public String dump(String indent) {
        StringBuilder sb = new StringBuilder();

        if (getClass().getEnclosingClass() != null)
            sb.append(indent + getClass().getEnclosingClass().getSimpleName() + "." + getClass().getSimpleName());
        else
            sb.append(indent + getClass().getSimpleName());

        if (downstream != null)
            sb.append(" [" + ((DelegateDownstream)downstream).dump() + ']');

        sb.append(System.lineSeparator());

        if (sources != null) {
            for (Node<Row> src : sources)
                sb.append(((AbstractNode<Row>)src).dump(indent + "  "));
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public List<Node<Row>> sources() {
        return sources;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (isClosed())
            return;

        closeInternal();

        if (!F.isEmpty(sources()))
            sources().forEach(U::closeQuiet);
    }

    /** {@inheritDoc} */
    @Override public void rewind() {
        rewindInternal();

        if (!F.isEmpty(sources()))
            sources().forEach(Node::rewind);
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Row> downstream) {
        this.downstream = downstream;
    }

    /**
     * Processes given exception.
     *
     * @param e Exception.
     */
    public void onError(Throwable e) {
        onErrorInternal(e);
    }

    /** */
    protected void closeInternal() {
        closed = true;
    }

    /** */
    protected abstract void rewindInternal();

    /** */
    protected void onErrorInternal(Throwable e) {
        Downstream<Row> downstream = downstream();

        assert downstream != null;

        try {
            downstream.onError(e);
        }
        finally {
            U.closeQuiet(this);
        }
    }

    /**
     * @return {@code true} if the subtree is canceled.
     */
    protected boolean isClosed() {
        return closed;
    }

    /** */
    protected void checkState() throws Exception {
        if (context().isCancelled())
            throw new QueryCancelledException();
        if (context().isTimedOut())
            throw new QueryCancelledException("The query was timed out.");
        if (Thread.interrupted())
            throw new IgniteInterruptedCheckedException("Thread was interrupted.");
    }

    /** */
    protected abstract Downstream<Row> requestDownstream(int idx);

    /** */
    @Override public Downstream<Row> downstream() {
        return downstream;
    }

    /** */
    private class DelegateDownstream implements Downstream<Row> {
        /** */
        private final Downstream<Row> delegate;

        /** */
        private long pushed;

        /** */
        private boolean ended;

        /** */
        private Throwable error;

        /** */
        public DelegateDownstream(Downstream<Row> delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void push(Row row) throws Exception {
            pushed++;
            delegate.push(row);
        }

        /** {@inheritDoc} */
        @Override public void end() throws Exception {
            ended = true;
            delegate.end();
        }

        /** {@inheritDoc} */
        @Override public void onError(Throwable e) {
            error = e;
            delegate.onError(e);
        }

        /** */
        private String dump() {
            return "pushed=" + pushed + ", ended=" + ended + ", error=" + error;
        }
    }
}

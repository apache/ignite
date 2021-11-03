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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionCancelledException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<RowT> implements Node<RowT> {
    /**
     *
     */
    protected static final int MODIFY_BATCH_SIZE = 100; //IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_BATCH_SIZE", 100);

    /**
     *
     */
    protected static final int IO_BATCH_SIZE = 256; //IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IO_BATCH_SIZE", 256);

    /**
     *
     */
    protected static final int IO_BATCH_CNT = 4; //IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IO_BATCH_CNT", 4);

    /**
     *
     */
    protected final int inBufSize = Commons.IN_BUFFER_SIZE; //IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE", 2);

    /**
     *
     */
    protected final IgniteLogger log = IgniteLogger.forClass(getClass());

    /** for debug purpose */
    private volatile Thread thread;

    /**
     * {@link Inbox} node may not have proper context at creation time in case it creates on first message received from a remote source.
     * This case the context sets in scope of {@link Inbox#init(ExecutionContext, RelDataType, Collection, Comparator)} method call.
     */
    private ExecutionContext<RowT> ctx;

    /**
     *
     */
    private RelDataType rowType;

    /**
     *
     */
    private Downstream<RowT> downstream;

    /**
     *
     */
    private boolean closed;

    /**
     *
     */
    private List<Node<RowT>> sources;

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext<RowT> ctx, RelDataType rowType) {
        this.ctx = ctx;
        this.rowType = rowType;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContext<RowT> context() {
        return ctx;
    }

    /**
     *
     */
    protected void context(ExecutionContext<RowT> ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowType() {
        return rowType;
    }

    /**
     *
     */
    protected void rowType(RelDataType rowType) {
        this.rowType = rowType;
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        this.sources = sources;

        for (int i = 0; i < sources.size(); i++) {
            sources.get(i).onRegister(requestDownstream(i));
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<Node<RowT>> sources() {
        return sources;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (isClosed()) {
            return;
        }

        closeInternal();

        if (!nullOrEmpty(sources())) {
            sources().forEach(Commons::closeQuiet);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void rewind() {
        rewindInternal();

        if (!nullOrEmpty(sources())) {
            sources().forEach(Node::rewind);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        this.downstream = downstream;
    }

    /**
     * Processes given exception.
     *
     * @param e Exception.
     */
    public void onError(Throwable e) {
        if (e instanceof ExecutionCancelledException) {
            log.warn("Execution is cancelled.", e);
        } else {
            onErrorInternal(e);
        }
    }

    /**
     *
     */
    protected void closeInternal() {
        closed = true;
    }

    /**
     *
     */
    protected abstract void rewindInternal();

    /**
     *
     */
    protected void onErrorInternal(Throwable e) {
        Downstream<RowT> downstream = downstream();

        assert downstream != null;

        try {
            downstream.onError(e);
        } finally {
            Commons.closeQuiet(this);
        }
    }

    /**
     * @return {@code true} if the subtree is canceled.
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     *
     */
    protected void checkState() throws Exception {
        if (context().isCancelled()) {
            throw new ExecutionCancelledException();
        }
        if (Thread.interrupted()) {
            throw new IgniteInternalCheckedException("Thread was interrupted.");
        }
        if (!IgniteUtils.assertionsEnabled()) {
            return;
        }
        if (thread == null) {
            thread = Thread.currentThread();
        } else {
            assert thread == Thread.currentThread();
        }
    }

    /**
     *
     */
    protected abstract Downstream<RowT> requestDownstream(int idx);

    /**
     *
     */
    @Override
    public Downstream<RowT> downstream() {
        return downstream;
    }
}

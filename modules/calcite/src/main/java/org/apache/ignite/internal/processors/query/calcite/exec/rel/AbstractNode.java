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

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<Row> implements Node<Row> {
    /** */
    protected static final int IN_BUFFER_SIZE = IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE", 512);

    /** */
    protected static final int MODIFY_BATCH_SIZE = IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_BATCH_SIZE", 100);

    /** */
    protected static final int IO_BATCH_SIZE = IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IO_BATCH_SIZE", 200);

    /** */
    protected static final int IO_BATCH_CNT = IgniteSystemProperties.getInteger("IGNITE_CALCITE_EXEC_IO_BATCH_CNT", 50);

    /** for debug purpose */
    private volatile Thread thread;

    /**
     * {@link Inbox} node may not have proper context at creation time in case it
     * creates on first message received from a remote source. This case the context
     * sets in scope of {@link Inbox#init(ExecutionContext, Collection, Comparator)} method call.
     */
    protected ExecutionContext<Row> ctx;

    /** */
    protected Downstream<Row> downstream;

    /** */
    protected List<Node<Row>> sources;

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext<Row> ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public ExecutionContext<Row> context() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        this.sources = sources;

        for (int i = 0; i < sources.size(); i++)
            sources.get(i).onRegister(requestDownstream(i));
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Row> downstream) {
        this.downstream = downstream;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();

        context().markCancelled();

        if (!F.isEmpty(sources))
            sources.forEach(Node::cancel);
    }

    /** */
    protected abstract Downstream<Row> requestDownstream(int idx);

    /** */
    protected void checkThread() {
        if (!U.assertionsEnabled())
            return;

        if (thread == null)
            thread = Thread.currentThread();
        else
            assert thread == Thread.currentThread();
    }
}

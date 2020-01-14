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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<T> implements Node<T> {
    /** for debug purpose */
    private volatile Thread thread;

    /** */
    private final ImmutableList<Node<T>> inputs;

    /** */
    private final SinkProxy target;

    /**
     * {@link Inbox} node may not have proper context at creation time in case it
     * creates on first message received from a remote source. This case the context
     * sets in scope of {@link Inbox#init(ExecutionContext, Collection, Comparator)} method call.
     */
    private volatile ExecutionContext ctx;

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext ctx) {
        this(ctx, ImmutableList.of());
    }

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext ctx, @NotNull Node<T> input) {
        this(ctx, ImmutableList.of(input));
    }

    /**
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext ctx, @NotNull List<Node<T>> inputs) {
        this.ctx = ctx;
        this.inputs = ImmutableList.copyOf(inputs);

        target = new SinkProxy();
    }

    /** {@inheritDoc} */
    @Override public void target(Sink<T> sink) {
        target.delegate(sink);
    }

    /** {@inheritDoc} */
    @Override public Sink<T> target() {
        return target;
    }

    /** {@inheritDoc} */
    @Override public List<Node<T>> inputs() {
        return inputs;
    }

    /** */
    protected void context(ExecutionContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public ExecutionContext context() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public void request() {
        checkThread();

        inputs().forEach(Node::request);
    }

    @Override public void cancel() {
        checkThread();

        context().setCancelled();
        inputs().forEach(Node::cancel);
    }

    @Override public void reset() {
        checkThread();

        inputs().forEach(Node::reset);
    }

    /**
     * Links the node inputs to the node sinks.
     */
    protected void link() {
        for (int i = 0; i < inputs.size(); i++)
            inputs.get(i).target(sink(i));
    }

    /** */
    protected void checkThread() {
        if (!U.assertionsEnabled())
            return;

        if (thread == null)
            thread = Thread.currentThread();
        else
            assert thread == Thread.currentThread();
    }

    /**
     * Fixes a race when a first message from remote node received at the time an execution tree is being built.
     */
    private class SinkProxy implements Sink<T> {
        /** */
        private boolean isEnd;

        /** */
        private Sink<T> delegate;

        /** {@inheritDoc} */
        @Override public boolean push(T row) {
            checkThread();

            Sink<T> target;
            boolean isEnd;

            synchronized (this) {
                target = delegate;
                isEnd = this.isEnd;
            }

            return !isEnd && target != null && target.push(row);
        }

        /** {@inheritDoc} */
        @Override public void end() {
            checkThread();

            Sink<T> target;

            synchronized (this) {
                if(isEnd)
                    return;

                isEnd = true;
                target = delegate;
            }

            if (target != null)
                target.end();
        }

        /** {@inheritDoc} */
        private void delegate(Sink<T> delegate){
            boolean isEnd;

            synchronized (this) {
                assert this.delegate == null;

                this.delegate = delegate;
                isEnd = this.isEnd;
            }

            if (isEnd)
                context().execute(delegate::end);
        }
    }
}

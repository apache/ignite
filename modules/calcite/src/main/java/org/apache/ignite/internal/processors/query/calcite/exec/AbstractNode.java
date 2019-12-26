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
import org.jetbrains.annotations.NotNull;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<T> implements Node<T> {
    /** */
    private final ImmutableList<Node<T>> inputs;

    /** */
    private volatile Sink<T> target;

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
    }

    /** {@inheritDoc} */
    @Override public void target(Sink<T> sink) {
        target = sink;
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

    /**
     * Links the node inputs to the node sinks.
     */
    protected void link() {
        for (int i = 0; i < inputs.size(); i++)
            inputs.get(i).target(sink(i));
    }
}

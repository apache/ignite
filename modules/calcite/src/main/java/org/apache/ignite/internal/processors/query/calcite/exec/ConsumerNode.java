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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.ignite.IgniteInterruptedException;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12449
 */
public class ConsumerNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Sink<Object[]>, Iterator<Object[]> {
    /** */
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    /** */
    private final int bufferSize;

    /** */
    private final ArrayBlockingQueue<Object> buff;

    /** */
    private Object cur;

    /**
     * @param ctx Execution context.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input) {
       this(ctx, input, DEFAULT_BUFFER_SIZE);
    }

    /**
     * @param ctx Execution context.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input, int bufferSize) {
        super(ctx, input);

        this.bufferSize = bufferSize;

        buff = new ArrayBlockingQueue<>(bufferSize + 1);

        link();
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request() {
        context().execute(input()::request);
    }

    /** {@inheritDoc} */
    @Override public boolean push(Object[] row) {
        if (buff.size() == bufferSize)
            return false;

        try {
            buff.put(row);
        }
        catch (InterruptedException e) {
            throw new AssertionError();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void end() {
        try {
            buff.put(EndMarker.INSTANCE);
        }
        catch (InterruptedException e) {
            throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (cur == EndMarker.INSTANCE)
            return false;

        if (cur == null)
            cur = take0();

        assert cur != null;

        return cur != EndMarker.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public Object[] next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Object tmp = cur;
        cur = null;

        return (Object[]) tmp;
    }

    private Object take0() {
        if (buff.isEmpty())
            request();

        try {
            return buff.take();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }
}

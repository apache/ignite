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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Client iterator.
 */
public class ConsumerNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Sink<Object[]>, Iterator<Object[]>, AutoCloseable {
    /** */
    private static final int DEFAULT_BUFFER_SIZE = 1000;
    
    /** */
    private enum State {
        RUNNING, CANCELLED, END
    }

    /** */
    private final ReentrantLock lock;

    /** */
    private final Condition cond;

    /** */
    private final int bufferSize;

    /** */
    private final ArrayDeque<Object> buff;

    /** */
    private final CloseListener<ConsumerNode> lsnr;

    /** */
    private Object cur;

    /** */
    private volatile State state = State.RUNNING;

    /**
     * @param ctx Execution context.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input) {
        this(ctx, input, DEFAULT_BUFFER_SIZE);
    }

    /**
     * @param ctx Execution context.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input, CloseListener<ConsumerNode> lsnr) {
        this(ctx, input, DEFAULT_BUFFER_SIZE, lsnr);
    }

    /**
     * @param ctx        Execution context.
     * @param input      Input node.
     * @param bufferSize Buffer size.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input, int bufferSize) {
        this(ctx, input, bufferSize, null);
    }

    /**
     * @param ctx Execution context.
     */
    public ConsumerNode(ExecutionContext ctx, Node<Object[]> input, int bufferSize, CloseListener<ConsumerNode> lsnr) {
        super(ctx, input);

        this.bufferSize = bufferSize;
        this.lsnr = lsnr;

        // extra space for possible END marker
        buff = new ArrayDeque<>(bufferSize + 1);
        lock = new ReentrantLock();
        cond = lock.newCondition();

        link();
    }
    
    public UUID queryId() {
        return context().queryId();
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
    @Override public void cancel() {
        if (state != State.RUNNING)
            return;

        lock.lock();
        try {
            if (state != State.RUNNING)
                return;

            state = State.CANCELLED;
            buff.clear();
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
        
        context().setCancelled();
        context().execute(input()::cancel);

        if (lsnr != null)
            lsnr.onClose(this);
    }
    
    public boolean canceled() {
        return state == State.CANCELLED;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cancel();
    }

    /** {@inheritDoc} */
    @Override public boolean push(Object[] row) {
        if (state != State.RUNNING)
            return false;

        lock.lock();
        try {
            if (state != State.RUNNING || buff.size() == bufferSize)
                return false;

            buff.offer(row);
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void end() {
        if (state != State.RUNNING)
            return;

        lock.lock();
        try {
            if (state != State.RUNNING)
                return;

            buff.offer(EndMarker.INSTANCE);
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (cur != null)
            return true;
        else if (state == State.END)
            return false;
        else
            return (cur = take()) != null;
    }

    /** {@inheritDoc} */
    @Override public Object[] next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Object cur0 = cur;
        cur = null;

        return (Object[]) cur0;
    }

    /** */
    private Object take() {
        if (state == State.CANCELLED)
            throw U.convertException(new QueryCancelledException());

        lock.lock();
        try {
            if (state == State.CANCELLED)
                throw U.convertException(new QueryCancelledException());

            assert state == State.RUNNING;

            while (buff.isEmpty()) {
                request();

                cond.await();

                if (state == State.CANCELLED)
                    throw U.convertException(new QueryCancelledException());

                assert state == State.RUNNING;
            }

            Object row = buff.poll();

            if (row != EndMarker.INSTANCE)
                return row;

            state = State.END;
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            lock.unlock();
        }

        assert state == State.END;

        if (lsnr != null)
            lsnr.onClose(this);
        
        return null;
    }
}

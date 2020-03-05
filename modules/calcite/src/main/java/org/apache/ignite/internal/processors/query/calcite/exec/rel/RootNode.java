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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.EndMarker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Client iterator.
 */
public class RootNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Downstream<Object[]>, Iterator<Object[]>, AutoCloseable {
    /** */
    public enum State {
        RUNNING, CANCELLED, END
    }

    /** */
    private final ReentrantLock lock;

    /** */
    private final Condition cond;

    /** */
    private final ArrayDeque<Object> buff;

    /** */
    private final Consumer<RootNode> onClose;

    /** */
    private volatile State state = State.RUNNING;

    /** */
    private volatile IgniteSQLException ex;

    /** */
    private Object row;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext ctx, Consumer<RootNode> onClose) {
        super(ctx);

        this.onClose = onClose;

        // extra space for possible END marker
        buff = new ArrayDeque<>(IN_BUFFER_SIZE + 1);
        lock = new ReentrantLock();
        cond = lock.newCondition();
    }
    
    public UUID queryId() {
        return context().queryId();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        if (state != State.RUNNING)
            return;

        lock.lock();
        try {
            if (state != State.RUNNING)
                return;

            context().markCancelled();
            state = State.CANCELLED;
            buff.clear();
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
        
        context().execute(F.first(sources)::cancel);
        onClose.accept(this);
    }

    /**
     * @return Execution state.
     */
    public State state() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cancel();
    }

    /** {@inheritDoc} */
    @Override public void push(Object[] row) {
        checkThread();

        int request = 0;

        lock.lock();
        try {
            assert waiting > 0;

            waiting--;

            if (state != State.RUNNING)
                return;

            buff.offer(row);

            if (waiting == 0)
                waiting = request = IN_BUFFER_SIZE - buff.size();

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        if (request > 0)
            F.first(sources).request(request);
    }

    /** {@inheritDoc} */
    @Override public void end() {
        lock.lock();
        try {
            assert waiting > 0;

            waiting = -1;

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
    @Override public void onError(Throwable e) {
        checkThread();

        ex = new IgniteSQLException("An error occurred while query executing.", IgniteQueryErrorCode.UNKNOWN, e);

        cancel();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (row != null)
            return true;
        else if (state == State.END)
            return false;
        else
            return (row = take()) != null;
    }

    /** {@inheritDoc} */
    @Override public Object[] next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Object cur0 = row;
        row = null;

        return (Object[]) cur0;
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Object[]> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        throw new UnsupportedOperationException();
    }

    /** */
    private Object take() {
        lock.lock();
        try {
            checkCancelled();

            assert state == State.RUNNING;

            while (buff.isEmpty()) {
                requestIfNeeded();

                cond.await();

                checkCancelled();

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

        onClose.accept(this);
        
        return null;
    }

    /** */
    private void checkCancelled() {
        if (state == State.CANCELLED) {
            if (ex != null)
                throw ex;

            throw new IgniteSQLException("The query was cancelled while executing.", IgniteQueryErrorCode.QUERY_CANCELED);
        }
    }

    /** */
    private void requestIfNeeded() {
        assert !F.isEmpty(sources) && sources.size() == 1;

        assert lock.isHeldByCurrentThread();

        if (waiting != 0)
            return;

        int request = waiting = IN_BUFFER_SIZE - buff.size();

        if (request > 0)
            context().execute(() -> F.first(sources).request(request));
    }
}

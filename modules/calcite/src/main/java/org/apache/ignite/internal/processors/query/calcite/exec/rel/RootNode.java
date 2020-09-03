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
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Client iterator.
 */
public class RootNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row>, Iterator<Row> {
    /** */
    private final ReentrantLock lock;

    /** */
    private final Condition cond;

    /** */
    private final Deque<Row> buff;

    /** */
    private final Runnable onClose;

    /** */
    private volatile State state = State.RUNNING;

    /** */
    private final AtomicReference<Throwable> ex = new AtomicReference<>();

    /** */
    private Row row;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<Row> ctx) {
        this(ctx, null);
    }

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<Row> ctx, Runnable onClose) {
        super(ctx);

        buff = new ArrayDeque<>(IN_BUFFER_SIZE);
        lock = new ReentrantLock();
        cond = lock.newCondition();

        this.onClose = onClose;
    }

    /** */
    public UUID queryId() {
        return context().queryId();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        lock.lock();
        try {
            if (state == State.RUNNING)
                state = State.CANCELLED;
            else if (state == State.END)
                state = State.CLOSED;
            else
                return;

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        if (onClose == null)
            onClose();
        else
            onClose.run();
    }

    /** {@inheritDoc} */
    @Override protected boolean isClosed() {
        return state == State.CANCELLED || state == State.CLOSED;
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        context().execute(() -> {
            buff.clear();

            U.closeQuiet(super::close);
        });
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        assert waiting > 0;

        try {
            checkState();

            int req = 0;

            lock.lock();
            try {
                if (state != State.RUNNING)
                    return;

                waiting--;

                buff.offer(row);

                if (waiting == 0)
                    waiting = req = IN_BUFFER_SIZE - buff.size();

                cond.signalAll();
            }
            finally {
                lock.unlock();
            }

            if (req > 0)
                source().request(req);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        try {
            checkState();

            lock.lock();
            try {
                assert waiting > 0 : "waiting=" + waiting;

                waiting = -1;

                if (state != State.RUNNING)
                    return;

                cond.signalAll();
            }
            finally {
                lock.unlock();
            }
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        if (!ex.compareAndSet(null, e))
            ex.get().addSuppressed(e);

        U.closeQuiet(this);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (row != null)
            return true;
        else if (state == State.END || state == State.CLOSED)
            return false;
        else
            return (row = take()) != null;
    }

    /** {@inheritDoc} */
    @Override public Row next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Row cur0 = row;
        row = null;

        return cur0;
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Row> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void onRewind() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        throw new UnsupportedOperationException();
    }

    /** */
    private Row take() {
        assert !F.isEmpty(sources()) && sources().size() == 1;

        lock.lock();
        try {
            while (true) {
                checkCancelled();
                assert state == State.RUNNING;

                if (!buff.isEmpty())
                    return buff.poll();
                else if (waiting == -1)
                    break;
                else if (waiting == 0) {
                    int req = waiting = IN_BUFFER_SIZE;
                    context().execute(() -> source().request(req));
                }

                cond.await();
            }

            state = State.END;
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            lock.unlock();
        }

        assert state == State.END;

        close();
        
        return null;
    }

    /** */
    private void checkCancelled() {
        if (state == State.CANCELLED) {
            ex.compareAndSet(null, new IgniteSQLException("The query was cancelled while executing.", IgniteQueryErrorCode.QUERY_CANCELED));

            throw sqlException(ex.get());
        }
    }

    /** */
    private IgniteSQLException sqlException(Throwable e) {
        return e instanceof IgniteSQLException
            ? (IgniteSQLException)e
            : new IgniteSQLException("An error occurred while query executing.", IgniteQueryErrorCode.UNKNOWN, e);
    }

    /** */
    private enum State {
        RUNNING, CANCELLED, END, CLOSED
    }
}

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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Client iterator.
 */
public class RootNode<Row> extends AbstractNode<Row>
    implements SingleNode<Row>, Downstream<Row>, Iterator<Row>, AutoCloseable {
    /** */
    private final ReentrantLock lock;

    /** */
    private final Condition cond;

    /** */
    private final Deque<Row> buff;

    /** */
    private final Consumer<RootNode<Row>> onClose;

    /** */
    private volatile State state = State.RUNNING;

    /** */
    private volatile IgniteSQLException ex;

    /** */
    private Row row;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<Row> ctx, Consumer<RootNode<Row>> onClose) {
        super(ctx);

        this.onClose = onClose;

        // extra space for possible END marker
        buff = new ArrayDeque<>(IN_BUFFER_SIZE + 1);
        lock = new ReentrantLock();
        cond = lock.newCondition();
    }

    /** */
    public UUID queryId() {
        return context().queryId();
    }

    /** */
    public void closeExecutionTree() {
        checkThread();

        if (isClosed())
            return;

        buff.clear();

        super.close();
    }

    /**
     * @return Execution state.
     */
    public State state() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        lock.lock();
        try {
            if (state == State.RUNNING)
                state = State.CANCELLED;

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        onClose.accept(this);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        int req = 0;

        lock.lock();
        try {
            assert waiting > 0 : "waiting=" + waiting;

            waiting--;

            if (state != State.RUNNING)
                return;

            buff.offer(row);

            if (waiting == 0)
                waiting = req = IN_BUFFER_SIZE - buff.size();

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        if (req > 0)
            F.first(sources).request(req);
    }

    /** {@inheritDoc} */
    @Override public void end() {
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

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        System.out.println("+++ onError");

        assert Objects.isNull(ex) : "The error has been handled. Previous error: " + ex;

        if (e instanceof IgniteSQLException)
            ex = (IgniteSQLException)e;
        else
            ex = new IgniteSQLException("An error occurred while query executing.", IgniteQueryErrorCode.UNKNOWN, e);

        close();
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
    @Override public void request(int rowsCnt) {
        throw new UnsupportedOperationException();
    }

    /** */
    private Row take() {
        assert !F.isEmpty(sources) && sources.size() == 1;

        lock.lock();
        try {
            checkCancelled();
            assert state == State.RUNNING;

            while (true) {
                if (!buff.isEmpty())
                    return buff.poll();
                else if (waiting == -1)
                    break;
                else if (waiting == 0) {
                    int req = waiting = IN_BUFFER_SIZE;
                    context().execute(() -> F.first(sources).request(req));
                }

                cond.await();

                checkCancelled();
                assert state == State.RUNNING;
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
    public enum State {
        /** */
        RUNNING,

        /** */
        CANCELLED,

        /** */
        END
    }
}

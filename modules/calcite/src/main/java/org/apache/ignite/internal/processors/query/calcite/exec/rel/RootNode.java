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
import java.util.function.Function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.query.QueryCancelledException.ERR_MSG;

/**
 * Client iterator.
 */
public class RootNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row>, Iterator<Row> {
    /** */
    private final ReentrantLock lock = new ReentrantLock();

    /** */
    private final Condition cond = lock.newCondition();

    /** */
    private final Runnable onClose;

    /** */
    private final AtomicReference<Throwable> ex = new AtomicReference<>();

    /** */
    private final Function<Row, Row> converter;

    /** */
    private int waiting;

    /** */
    private Deque<Row> inBuff = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private Deque<Row> outBuff = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private volatile boolean closed;

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        super(ctx, rowType);

        onClose = this::closeInternal;
        converter = TypeUtils.resultTypeConverter(ctx, rowType);
    }

    /**
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<Row> ctx, RelDataType rowType, Runnable onClose) {
        super(ctx, rowType);

        this.onClose = onClose;
        converter = TypeUtils.resultTypeConverter(ctx, rowType);
    }

    /** */
    public UUID queryId() {
        return context().queryId();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed)
            return;

        lock.lock();
        try {
            if (waiting != -1)
                ex.compareAndSet(null, new IgniteSQLException(ERR_MSG, IgniteQueryErrorCode.QUERY_CANCELED));

            closed = true; // an exception has to be set first to get right check order

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }

        onClose.run();
    }

    /** {@inheritDoc} */
    @Override protected boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void closeInternal() {
        context().execute(() -> sources().forEach(U::closeQuiet), this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        lock.lock();
        try {
            assert waiting > 0;

            checkState();

            waiting--;

            inBuff.offer(row);

            if (inBuff.size() == IN_BUFFER_SIZE)
                cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert waiting > 0;

        lock.lock();
        try {
            checkState();

            waiting = -1;

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onErrorInternal(Throwable e) {
        if (!ex.compareAndSet(null, e))
            ex.get().addSuppressed(e);

        U.closeQuiet(this);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        checkException();

        if (!outBuff.isEmpty())
            return true;

        if (closed && ex.get() == null)
            return false;

        exchangeBuffers();

        return !outBuff.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public Row next() {
        if (!hasNext())
            throw new NoSuchElementException();

        return converter.apply(outBuff.remove());
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
    @Override protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        throw new UnsupportedOperationException();
    }

    /** */
    private void exchangeBuffers() {
        assert !F.isEmpty(sources()) && sources().size() == 1;

        lock.lock();
        try {
            while (ex.get() == null) {
                assert outBuff.isEmpty();

                if (inBuff.size() == IN_BUFFER_SIZE || waiting == -1) {
                    Deque<Row> tmp = inBuff;
                    inBuff = outBuff;
                    outBuff = tmp;
                }

                if (waiting == -1)
                    close();
                else if (inBuff.isEmpty() && waiting == 0) {
                    int req = waiting = IN_BUFFER_SIZE;
                    context().execute(() -> source().request(req), this::onError);
                }

                if (!outBuff.isEmpty() || waiting == -1)
                    break;

                cond.await();
            }
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
        finally {
            lock.unlock();
        }

        checkException();
    }

    /** */
    private void checkException() {
        Throwable e = ex.get();

        if (e == null)
            return;

        if (e instanceof IgniteSQLException)
            throw (IgniteSQLException)e;
        else
            throw new IgniteSQLException("An error occurred while query executing.", IgniteQueryErrorCode.UNKNOWN, e);
    }
}

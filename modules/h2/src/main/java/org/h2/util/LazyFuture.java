/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.h2.message.DbException;

/**
 * Single threaded lazy future.
 *
 * @author Sergi Vladykin
 *
 * @param <T> the result type
 */
public abstract class LazyFuture<T> implements Future<T> {

    private static final int S_READY = 0;
    private static final int S_DONE = 1;
    private static final int S_ERROR = 2;
    private static final int S_CANCELED = 3;

    private int state = S_READY;
    private T result;
    private Exception error;

    /**
     * Reset this future to the initial state.
     *
     * @return {@code false} if it was already in initial state
     */
    public boolean reset() {
        if (state == S_READY) {
            return false;
        }
        state = S_READY;
        result = null;
        error = null;
        return true;
    }

    /**
     * Run computation and produce the result.
     *
     * @return the result of computation
     */
    protected abstract T run() throws Exception;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (state != S_READY) {
            return false;
        }
        state = S_CANCELED;
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        switch (state) {
        case S_READY:
            try {
                result = run();
                state = S_DONE;
            } catch (Exception e) {
                error = e;
                if (e instanceof InterruptedException) {
                    throw (InterruptedException) e;
                }
                throw new ExecutionException(e);
            } finally {
                if (state != S_DONE) {
                    state = S_ERROR;
                }
            }
            return result;
        case S_DONE:
            return result;
        case S_ERROR:
            throw new ExecutionException(error);
        case S_CANCELED:
            throw new CancellationException();
        default:
            throw DbException.throwInternalError("" + state);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
        return get();
    }

    @Override
    public boolean isCancelled() {
        return state == S_CANCELED;
    }

    @Override
    public boolean isDone() {
        return state != S_READY;
    }
}

/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future which is already done.
 *
 * @param <T> Result value.
 * @author Sergi Vladykin
 */
public class DoneFuture<T> implements Future<T> {
    final T x;

    public DoneFuture(T x) {
        this.x = x;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return x;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return x;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public String toString() {
        return "DoneFuture->" + x;
    }
}

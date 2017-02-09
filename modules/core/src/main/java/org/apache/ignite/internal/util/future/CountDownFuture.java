/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.util.future;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CountDownFuture extends GridFutureAdapter<Void> {
    /** */
    private AtomicInteger remaining;

    /** */
    private AtomicReference<Exception> errCollector;

    /**
     * @param cnt Number of completing parties.
     */
    public CountDownFuture(int cnt) {
        remaining = new AtomicInteger(cnt);
        errCollector = new AtomicReference<>();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (err != null)
            addError(err);

        int left = remaining.decrementAndGet();

        boolean done = left == 0 && super.onDone(res, errCollector.get());

        if (done)
            afterDone();

        return done;
    }

    /**
     *
     */
    protected void afterDone() {
        // No-op, to be overridden in subclasses.
    }

    /**
     * @param err Error.
     */
    private void addError(Throwable err) {
        Exception ex = errCollector.get();

        if (ex == null) {
            Exception compound = new IgniteCheckedException("Compound exception for CountDownFuture.");

            ex = errCollector.compareAndSet(null, compound) ? compound : errCollector.get();
        }

        assert ex != null;

        ex.addSuppressed(err);
    }
}

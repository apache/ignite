/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Future that represents already completed result.
 */
public class GridNioFinishedFuture<R> implements GridNioFuture<R> {
    /** Future result. */
    private R res;

    /** Future exception. */
    private Throwable err;

    /** Message thread flag. */
    private boolean msgThread;

    /**
     * Constructs a future which {@link #get()} method will return a given result.
     *
     * @param res Future result.
     */
    public GridNioFinishedFuture(R res) {
        this.res = res;
    }

    /**
     * Constructs a future which {@link #get()} method will throw given exception.
     *
     * @param err Exception to be thrown.
     */
    public GridNioFinishedFuture(@Nullable Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IOException, GridException {
        if (err != null) {
            if (err instanceof IOException)
                throw (IOException)err;

            throw U.cast(err);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws IOException, GridException {
        return get();
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IOException, GridException {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(@Nullable IgniteInClosure<? super GridNioFuture<R>> lsnr) {
        if (lsnr != null)
            lsnr.apply(this);
    }

    /** {@inheritDoc} */
    @Override public void messageThread(boolean msgThread) {
        this.msgThread = msgThread;
    }

    /** {@inheritDoc} */
    @Override public boolean messageThread() {
        return msgThread;
    }
}

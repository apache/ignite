/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Executes initialization operation once.
 */
public class GridAtomicInitializer<T> {
    /** */
    private final Object mux = new Object();

    /** */
    private volatile boolean finished;

    /** Don't use volatile because we write this field before 'finished' write and read after 'finished' read. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private Exception e;

    /** Don't use volatile because we write this field before 'finished' write and read after 'finished' read. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private T res;

    /**
     * Executes initialization operation only once.
     *
     * @param c Initialization operation.
     * @return Result of initialization.
     * @throws IgniteCheckedException If failed.
     */
    public T init(Callable<T> c) throws IgniteCheckedException {
        if (!finished) {
            synchronized (mux) {
                if (!finished) {
                    try {
                        res = c.call();
                    }
                    catch (Exception e) {
                        this.e = e;
                    }
                    finally {
                        finished = true;

                        mux.notifyAll();
                    }
                }
            }
        }

        if (e != null)
            throw e instanceof IgniteCheckedException ? (IgniteCheckedException)e : new IgniteCheckedException(e);

        return res;
    }

    /**
     * @return True, if initialization was already successfully completed.
     */
    public boolean succeeded() {
        return finished && e == null;
    }

    /**
     * Should be called only if succeeded.
     *
     * @return Result.
     */
    public T result() {
        return res;
    }

    /**
     * Await for completion.
     *
     * @return {@code true} If initialization was completed successfully.
     * @throws GridInterruptedException If thread was interrupted.
     */
    public boolean await() throws GridInterruptedException {
        if (!finished) {
            synchronized (mux) {
                while (!finished)
                    U.wait(mux);
            }
        }

        return e == null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAtomicInitializer.class, this);
    }
}

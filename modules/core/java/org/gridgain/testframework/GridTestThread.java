// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import java.util.concurrent.*;

/**
 * Test thread that has convenience failure checks.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"ProhibitedExceptionThrown", "CatchGenericClass"})
public class GridTestThread extends Thread {
    /** Error. */
    private Throwable err;

    /** Target runnable. */
    private final Runnable run;

    /** Target callable. */
    private final Callable<?> call;

    /**
     * @param run Target runnable.
     */
    @SuppressWarnings({"NullableProblems"})
    public GridTestThread(Runnable run) {
        this(run, null);
    }

    /**
     * @param call Target callable.
     */
    @SuppressWarnings({"NullableProblems"})
    public GridTestThread(Callable<?> call) {
        this(call, null);
    }

    /**
     * @param run Target runnable.
     * @param name Thread name.
     */
    public GridTestThread(Runnable run, String name) {
        assert run != null;

        this.run = run;

        call = null;

        if (name != null)
            setName(name);
    }

    /**
     * @param call Target callable.
     * @param name Thread name.
     */
    public GridTestThread(Callable<?> call, String name) {
        assert call != null;

        this.call = call;

        run = null;

        if (name != null)
            setName(name);
    }

    /** {@inheritDoc} */
    @Override public final void run() {
        try {
            if (call != null)
                call.call();
            else
                run.run();
        }
        catch (Throwable e) {
            System.err.println("Failure in thread: " + name0());

            e.printStackTrace();

            err = e;

            onError(e);
        }
        finally {
            onFinished();
        }
    }

    /**
     * Callback for subclasses.
     */
    protected void onFinished() {
        // No-op.
    }

    /**
     * Callback for subclasses.
     *
     * @param err Error.
     */
    protected void onError(Throwable err) {
        assert err != null;

        // No-op.
    }

    /**
     * @return Error.
     */
    public Throwable getError() {
        return err;
    }

    /**
     * @throws Exception If there is error.
     */
    public void checkError() throws Exception {
        if (err != null) {
            if (err instanceof Error)
                throw (Error)err;

            throw (Exception)err;
        }
    }

    /**
     * @return Formatted string for current thread.
     */
    private String name0() {
        return "Thread [id=" + Thread.currentThread().getId() + ", name=" + Thread.currentThread().getName() + ']';
    }
}

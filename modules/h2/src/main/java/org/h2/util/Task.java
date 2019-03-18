/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A method call that is executed in a separate thread. If the method throws an
 * exception, it is wrapped in a RuntimeException.
 */
public abstract class Task implements Runnable {

    private static final AtomicInteger counter = new AtomicInteger();

    /**
     * A flag indicating the get() method has been called.
     */
    public volatile boolean stop;

    /**
     * The result, if any.
     */
    private volatile Object result;

    private volatile boolean finished;

    private Thread thread;

    private volatile Exception ex;

    /**
     * The method to be implemented.
     *
     * @throws Exception any exception is wrapped in a RuntimeException
     */
    public abstract void call() throws Exception;

    @Override
    public void run() {
        try {
            call();
        } catch (Exception e) {
            this.ex = e;
        }
        finished = true;
    }

    /**
     * Start the thread.
     *
     * @return this
     */
    public Task execute() {
        return execute(getClass().getName() + ":" + counter.getAndIncrement());
    }

    /**
     * Start the thread.
     *
     * @param threadName the name of the thread
     * @return this
     */
    public Task execute(String threadName) {
        thread = new Thread(this, threadName);
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /**
     * Calling this method will set the stop flag and wait until the thread is
     * stopped.
     *
     * @return the result, or null
     * @throws RuntimeException if an exception in the method call occurs
     */
    public Object get() {
        Exception e = getException();
        if (e != null) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Whether the call method has returned (with or without exception).
     *
     * @return true if yes
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * Get the exception that was thrown in the call (if any).
     *
     * @return the exception or null
     */
    public Exception getException() {
        join();
        if (ex != null) {
            return ex;
        }
        return null;
    }

    /**
     * Stop the thread and wait until it is no longer running. Exceptions are
     * ignored.
     */
    public void join() {
        stop = true;
        if (thread == null) {
            throw new IllegalStateException("Thread not started");
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
    }

}

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

package org.apache.ignite.testframework;

import java.util.concurrent.Callable;

/**
 * Test thread that has convenience failure checks.
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
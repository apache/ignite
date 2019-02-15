/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
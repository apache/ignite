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

package org.apache.ignite.internal.util;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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
     * @throws IgniteInterruptedCheckedException If thread was interrupted.
     */
    public boolean await() throws IgniteInterruptedCheckedException {
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
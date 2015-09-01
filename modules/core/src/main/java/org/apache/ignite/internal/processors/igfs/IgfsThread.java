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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.util.typedef.X;

/**
 * IGFS ad-hoc thread.
 */
public abstract class IgfsThread extends Thread {
    /**
     * Creates {@code IGFS} add-hoc thread.
     */
    protected IgfsThread() {
        super("igfs-worker");
    }

    /**
     * Creates {@code IGFS} add-hoc thread.
     *
     * @param name Thread name.
     */
    protected IgfsThread(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public final void run() {
        try {
            body();
        }
        catch (InterruptedException ignore) {
            interrupt();
        }
        // Catch all.
        catch (Throwable e) {
            X.error("Failed to execute IGFS ad-hoc thread: " + e.getMessage());

            e.printStackTrace();

            if (e instanceof Error)
                throw e;
        }
        finally {
            try {
                cleanup();
            }
            // Catch all.
            catch (Throwable  e) {
                X.error("Failed to clean up IGFS ad-hoc thread: " + e.getMessage());

                e.printStackTrace();

                if (e instanceof Error)
                    throw e;
            }
        }
    }

    /**
     * Thread body.
     *
     * @throws InterruptedException If interrupted.
     */
    protected abstract void body() throws InterruptedException;

    /**
     * Cleanup.
     */
    protected void cleanup() {
        // No-op.
    }
}
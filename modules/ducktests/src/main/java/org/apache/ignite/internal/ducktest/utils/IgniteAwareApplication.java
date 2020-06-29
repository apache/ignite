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

package org.apache.ignite.internal.ducktest.utils;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class IgniteAwareApplication {
    /** Logger. */
    protected static final Logger log = LogManager.getLogger(IgniteAwareApplication.class.getName());

    /** App inited. */
    private static final String APP_INITED = "IGNITE_APPLICATION_INITIALIZED";

    /** App finished. */
    private static final String APP_FINISHED = "IGNITE_APPLICATION_FINISHED";

    /** App terminated. */
    private static final String APP_TERMINATED = "IGNITE_APPLICATION_TERMINATED";

    /** Inited. */
    private static volatile boolean inited;

    /** Finished. */
    private static volatile boolean finished;

    /** Terminated. */
    private static volatile boolean terminated;

    /** Ignite. */
    protected final Ignite ignite;

    /**
     * Default constructor.
     */
    protected IgniteAwareApplication() {
        ignite = null;
    }

    /**
     *
     */
    protected IgniteAwareApplication(Ignite ignite) {
        this.ignite = ignite;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            terminate();

            while (!finished()) {
                log.info("Waiting for graceful termnation.");

                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }

            log.info("SIGTERM recorded.");
        }));

        log.info("ShutdownHook registered.");
    }

    /**
     * Used to marks as started to perform actions. Suitable for async runs.
     */
    protected void markInitialized() {
        assert !inited;

        log.info(APP_INITED);

        inited = true;
    }

    /**
     *
     */
    protected void markFinished() {
        assert !finished;

        log.info(APP_FINISHED);

        finished = true;
    }

    /**
     *
     */
    protected void markSyncExecutionComplete() {
        markInitialized();
        markFinished();
    }

    /**
     *
     */
    private boolean finished() {
        return finished;
    }

    /**
     *
     */
    private void terminate() {
        assert !terminated;

        log.info(APP_TERMINATED);

        terminated = true;
    }

    /**
     *
     */
    protected boolean terminated() {
        return terminated;
    }

    /**
     * @param name Name.
     * @param val Value.
     */
    protected void recordResult(String name, String val) {
        assert !finished;

        log.info(name + "->" + val + "<-");
    }

    /**
     * @param name Name.
     * @param val Value.
     */
    protected void recordResult(String name, long val) {
        recordResult(name, String.valueOf(val));
    }

    /**
     *
     */
    protected abstract void run(String[] args) throws Exception;

    /**
     * @param args Args.
     */
    public void start(String[] args) {
        try {
            log.info("Application params: " + Arrays.toString(args));

            run(args);

            assert inited : "Was not properly initialized.";
            assert finished : "Was not properly finished.";
        }
        catch (Throwable th) {
            log.error("Unexpected Application failure... ", th);
        }
        finally {
            log.info("Application finished.");
        }
    }
}

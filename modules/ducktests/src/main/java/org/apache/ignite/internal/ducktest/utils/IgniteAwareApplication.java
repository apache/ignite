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

import com.fasterxml.jackson.databind.JsonNode;
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

    /** App broken. */
    private static final String APP_BROKEN = "IGNITE_APPLICATION_BROKEN";

    /** App terminated. */
    private static final String APP_TERMINATED = "IGNITE_APPLICATION_TERMINATED";

    /** Inited. */
    private static volatile boolean inited;

    /** Finished. */
    private static volatile boolean finished;

    /** Broken. */
    private static volatile boolean broken;

    /** Terminated. */
    private static volatile boolean terminated;

    /** Shutdown hook. */
    private static volatile Thread hook;

    /** Ignite. */
    protected Ignite ignite;

    /** Cfg path. */
    protected String cfgPath;

    /**
     * Default constructor.
     */
    protected IgniteAwareApplication() {
        Runtime.getRuntime().addShutdownHook(hook = new Thread(() -> {
            log.info("SIGTERM recorded.");

            if (!finished && !broken)
                terminate();
            else
                log.info("Application already done [finished=" + finished + ", broken=" + broken + "]");

            while (!finished && !broken) {
                log.info("Waiting for graceful termnation.");

                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }
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
        assert !broken;

        log.info(APP_FINISHED);

        removeShutdownHook();

        finished = true;
    }

    /**
     *
     */
    protected void markBroken() {
        assert !finished;
        assert !broken;

        log.info(APP_BROKEN);

        removeShutdownHook();

        broken = true;
    }

    /**
     *
     */
    private void removeShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(hook);

        log.info("Shutdown hook removed.");
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
     * @param jsonNode JSON node.
     */
    protected abstract void run(JsonNode jsonNode) throws Exception;

    /**
     * @param jsonNode JSON node.
     */
    public void start(JsonNode jsonNode) {
        try {
            log.info("Application params: " + jsonNode);

            assert cfgPath != null;

            run(jsonNode);

            assert inited : "Was not properly initialized.";
            assert finished : "Was not properly finished.";
        }
        catch (Throwable th) {
            log.error("Unexpected Application failure... ", th);

            recordResult("ERROR", th.getMessage());

            markBroken();
        }
        finally {
            log.info("Application finished.");
        }
    }
}

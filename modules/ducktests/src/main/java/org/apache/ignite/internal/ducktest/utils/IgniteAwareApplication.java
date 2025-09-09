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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Signal;

/**
 *
 */
public abstract class IgniteAwareApplication {
    /** Logger. */
    protected static final Logger log = LogManager.getLogger(IgniteAwareApplication.class);

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

    /** State mutex. */
    private static final Object stateMux = new Object();

    /** Ignite. */
    protected Ignite ignite;

    /** Client. */
    protected IgniteClient client;

    /** Thin JDBC DataSource. */
    protected IgniteJdbcThinDataSource thinJdbcDataSource;

    /** Cfg path. */
    protected String cfgPath;

    /**
     * Default constructor.
     */
    protected IgniteAwareApplication() {
        Signal.handle(new Signal("TERM"), signal -> {
            log.info("SIGTERM recorded.");

            if (!finished && !broken)
                terminate();
            else
                log.info("Application already done [finished=" + finished + ", broken=" + broken + "]");

            if (log.isDebugEnabled())
                log.debug("Waiting for graceful termination...");

            int iter = 0;

            while (!finished && !broken) {
                log.info("Waiting for graceful termination cycle... [iter=" + ++iter + "]");

                if (iter == 100)
                    dumpThreads();

                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }

            log.info("Application finished. Waiting for graceful termination.");
        });

        log.info("SIGTERM handler registered.");
    }

    /**
     * Used to marks as started to perform actions. Suitable for async runs.
     */
    protected void markInitialized() {
        log.info("Marking as initialized.");

        synchronized (stateMux) {
            assert !inited;
            assert !finished;
            assert !broken;

            log.info(APP_INITED);

            inited = true;
        }
    }

    /**
     *
     */
    protected void markFinished() {
        log.info("Marking as finished.");

        synchronized (stateMux) {
            assert inited;
            assert !finished;
            assert !broken;

            log.info(APP_FINISHED);

            finished = true;
        }
    }

    /**
     *
     */
    public void markBroken(Throwable th) {
        log.info("Marking as broken.");

        synchronized (stateMux) {
            recordResult("ERROR", th.toString());

            if (broken) {
                log.info("Already marked as broken.");

                return;
            }

            assert !finished;

            log.error(APP_BROKEN);

            broken = true;
        }
    }

    /**
     *
     */
    private void terminate() {
        log.info("Marking as terminated.");

        synchronized (stateMux) {
            assert !terminated;

            log.info(APP_TERMINATED);

            terminated = true;
        }
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
    protected boolean terminated() {
        return terminated;
    }

    /**
     *
     */
    protected boolean inited() {
        return inited;
    }

    /**
     *
     */
    protected boolean active() {
        return !(terminated || broken || finished);
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

            if (!broken)
                markBroken(th);
        }
        finally {
            log.info("Application finished.");
        }
    }

    /**
     *
     */
    private static void dumpThreads() {
        ThreadInfo[] infos = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);

        for (ThreadInfo info : infos) {
            log.info(info.toString());

            if ("main".equals(info.getThreadName())) {
                StringBuilder sb = new StringBuilder();

                sb.append("main\n");

                for (StackTraceElement element : info.getStackTrace()) {
                    sb.append("\tat ").append(element.toString());
                    sb.append('\n');
                }

                log.info(sb.toString());
            }
        }
    }

    /**
     *
     */
    protected void waitForActivation() throws IgniteInterruptedCheckedException {
        boolean newApi = ignite.cluster().localNode().version().greaterThanEqual(2, 9, 0);

        while (newApi ? ignite.cluster().state() != ClusterState.ACTIVE : !ignite.cluster().active()) {
            U.sleep(100);

            log.info("Waiting for cluster activation");
        }

        log.info("Cluster Activated");
    }

    /**
     *
     */
    protected void waitForRebalanced() throws IgniteInterruptedCheckedException {
        boolean possible = ignite.cluster().localNode().version().greaterThanEqual(2, 8, 0);

        if (possible) {
            GridCachePartitionExchangeManager<?, ?> mgr = ((IgniteEx)ignite).context().cache().context().exchange();

            while (!mgr.lastFinishedFuture().rebalanced()) {
                U.sleep(1000);

                log.info("Waiting for cluster rebalance finish");
            }

            log.info("Cluster Rebalanced");
        }
        else
            throw new UnsupportedOperationException("Operation supported since 2.8.0");
    }
}

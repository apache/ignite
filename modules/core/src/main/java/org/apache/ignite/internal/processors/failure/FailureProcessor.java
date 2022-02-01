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

package org.apache.ignite.internal.processors.failure;

import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CorruptedDataStructureException;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE;

/**
 * General failure processing API
 */
public class FailureProcessor extends GridProcessorAdapter {
    /** @see IgniteSystemProperties#IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE */
    public static final int DFLT_FAILURE_HANDLER_RESERVE_BUFFER_SIZE = 64 * 1024;

    /** Value of the system property that enables threads dumping on failure. */
    private final boolean igniteDumpThreadsOnFailure =
        IgniteSystemProperties.getBoolean(IGNITE_DUMP_THREADS_ON_FAILURE, true);

    /** Timeout for throttling of thread dumps generation. */
    private long dumpThreadsTrottlingTimeout;

    /** Ignored failure log message. */
    static final String IGNORED_FAILURE_LOG_MSG = "Possible failure suppressed accordingly to a configured handler ";

    /** Failure log message. */
    static final String FAILURE_LOG_MSG = "Critical system error detected. " +
        "Will be handled accordingly to configured handler ";

    /** Thread dump per failure type timestamps. */
    private final Map<FailureType, Long> threadDumpPerFailureTypeTs;

    /** Ignite. */
    private final Ignite ignite;

    /** Handler. */
    private volatile FailureHandler hnd;

    /** Failure context. */
    private volatile FailureContext failureCtx;

    /** Reserve buffer, which can be dropped to handle OOME. */
    private volatile byte[] reserveBuf;

    /**
     * @param ctx Context.
     */
    public FailureProcessor(GridKernalContext ctx) {
        super(ctx);

        ignite = ctx.grid();

        Map<FailureType, Long> threadDumpPerFailureTypeTs = null;

        if (igniteDumpThreadsOnFailure) {
            dumpThreadsTrottlingTimeout =
                    IgniteSystemProperties.getLong(
                            IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT,
                            ctx.config().getFailureDetectionTimeout()
                    );

            if (dumpThreadsTrottlingTimeout > 0) {
                threadDumpPerFailureTypeTs = new EnumMap<>(FailureType.class);

                for (FailureType type : FailureType.values())
                    threadDumpPerFailureTypeTs.put(type, 0L);
            }
        }

        this.threadDumpPerFailureTypeTs = threadDumpPerFailureTypeTs;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        FailureHandler hnd = ctx.config().getFailureHandler();

        if (hnd == null)
            hnd = getDefaultFailureHandler();

        reserveBuf = new byte[IgniteSystemProperties.getInteger(
            IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE, DFLT_FAILURE_HANDLER_RESERVE_BUFFER_SIZE)];

        assert hnd != null;

        this.hnd = hnd;

        U.quietAndInfo(log, "Configured failure handler: [hnd=" + hnd + ']');
    }

    /**
     * @return @{code True} if a node will be stopped by current handler in near time.
     */
    public boolean nodeStopping() {
        return failureCtx != null && !(hnd instanceof NoOpFailureHandler);
    }

    /**
     * This method is used to initialize local failure handler if {@link IgniteConfiguration}
     * doesn't contain configured one.
     *
     * @return Default {@link FailureHandler} implementation.
     */
    protected FailureHandler getDefaultFailureHandler() {
        return new StopNodeOrHaltFailureHandler();
    }

    /**
     * @return Failure context.
     */
    public FailureContext failureContext() {
        return failureCtx;
    }

    /**
     * Processes failure accordingly to configured {@link FailureHandler}.
     *
     * @param failureCtx Failure context.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    public boolean process(FailureContext failureCtx) {
        return process(failureCtx, hnd);
    }

    /**
     * Processes failure accordingly to given failure handler.
     *
     * @param failureCtx Failure context.
     * @param hnd Failure handler.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    public synchronized boolean process(FailureContext failureCtx, FailureHandler hnd) {
        assert failureCtx != null;
        assert hnd != null;

        if (this.failureCtx != null) // Node already terminating, no reason to process more errors.
            return false;

        if (failureTypeIgnored(failureCtx, hnd)) {
            U.quietAndWarn(ignite.log(), IGNORED_FAILURE_LOG_MSG +
                "[hnd=" + hnd + ", failureCtx=" + failureCtx + ']', failureCtx.error());
        }
        else {
            U.error(ignite.log(), FAILURE_LOG_MSG +
                "[hnd=" + hnd + ", failureCtx=" + failureCtx + ']', failureCtx.error());
        }

        if (reserveBuf != null && X.hasCause(failureCtx.error(), OutOfMemoryError.class))
            reserveBuf = null;

        CorruptedDataStructureException corruptedDataStructureEx =
            X.cause(failureCtx.error(), CorruptedDataStructureException.class);

        if (corruptedDataStructureEx != null) {
            CacheGroupContext grpCtx = ctx.cache().cacheGroup(corruptedDataStructureEx.groupId());

            if (grpCtx != null && grpCtx.dataRegion() != null) {
                if (grpCtx.dataRegion().config().isPersistenceEnabled()) {
                    log.error("A critical problem with persistence data structures was detected." +
                        " Please make backup of persistence storage and WAL files for further analysis." +
                        " Persistence storage path: " + ctx.config().getDataStorageConfiguration().getStoragePath() +
                        " WAL path: " + ctx.config().getDataStorageConfiguration().getWalPath() +
                        " WAL archive path: " + ctx.config().getDataStorageConfiguration().getWalArchivePath());
                }
                else
                    log.error("A critical problem with in-memory data structures was detected.");
            }
        }

        if (igniteDumpThreadsOnFailure && !throttleThreadDump(failureCtx.type()))
            U.dumpThreads(log, !failureTypeIgnored(failureCtx, hnd));

        DiagnosticProcessor diagnosticProcessor = ctx.diagnostic();

        if (diagnosticProcessor != null)
            diagnosticProcessor.onFailure(failureCtx);

        boolean invalidated = hnd.onFailure(ignite, failureCtx);

        if (invalidated) {
            this.failureCtx = failureCtx;

            log.error("Ignite node is in invalid state due to a critical failure.");
        }

        return invalidated;
    }

    /**
     * Returns timeout for throttling of thread dumps generation.
     *
     * @return Timeout for throttling of thread dumps generation.
     */
    long dumpThreadsTrottlingTimeout() {
        return dumpThreadsTrottlingTimeout;
    }

    /**
     * Defines whether thread dump should be throttled for given failure type or not.
     *
     * @param type Failure type.
     * @return {@code True} if thread dump generation should be throttled fro given failure type.
     */
    private boolean throttleThreadDump(FailureType type) {
        if (dumpThreadsTrottlingTimeout <= 0)
            return false;

        long curr = System.currentTimeMillis();

        Long last = threadDumpPerFailureTypeTs.get(type);

        assert last != null : "Unknown failure type " + type;

        boolean throttle = curr - last < dumpThreadsTrottlingTimeout;

        if (!throttle)
            threadDumpPerFailureTypeTs.put(type, curr);
        else {
            if (log.isInfoEnabled()) {
                log.info("Thread dump is hidden due to throttling settings. " +
                        "Set IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT property to 0 to see all thread dumps.");
            }
        }

        return throttle;
    }

    /**
     * @param failureCtx Failure context.
     * @param hnd Handler.
     */
    private static boolean failureTypeIgnored(FailureContext failureCtx, FailureHandler hnd) {
        return hnd instanceof AbstractFailureHandler &&
            ((AbstractFailureHandler)hnd).getIgnoredFailureTypes().contains(failureCtx.type());
    }
}

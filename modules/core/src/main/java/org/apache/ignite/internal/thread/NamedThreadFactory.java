/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Named thread factory with prefix.
 */
public class NamedThreadFactory implements ThreadFactory {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(NamedThreadFactory.class);

    /** LogUncaughtExceptionHandler is used as default handler for uncaught exceptions. */
    private static final LogUncaughtExceptionHandler DFLT_LOG_UNCAUGHT_EX_HANDLER = new LogUncaughtExceptionHandler();

    /** Thread prefix. */
    private final String prefix;

    /** Thread counter. */
    private final AtomicInteger counter = new AtomicInteger(0);

    /** Thread daemon flag. */
    private final boolean daemon;

    /** Exception handler. */
    private final Thread.UncaughtExceptionHandler eHnd;

    /**
     * Constructor
     *
     * @param prefix Thread name prefix.
     */
    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }

    /**
     * Constructor
     *
     * @param prefix Thread name prefix.
     * @param daemon Daemon flag.
     */
    public NamedThreadFactory(String prefix, boolean daemon) {
        this(prefix, daemon, DFLT_LOG_UNCAUGHT_EX_HANDLER);
    }

    /**
     * Constructor
     *
     * @param prefix Thread name prefix.
     * @param daemon Daemon flag.
     * @param eHnd Uncaught exception handler.
     */
    public NamedThreadFactory(String prefix, boolean daemon, Thread.UncaughtExceptionHandler eHnd) {
        super();
        this.prefix = prefix;
        this.daemon = daemon;
        this.eHnd = eHnd != null ? eHnd : DFLT_LOG_UNCAUGHT_EX_HANDLER;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(Runnable r) {
        Thread t = new Thread(r);

        t.setDaemon(this.daemon);
        t.setUncaughtExceptionHandler(eHnd);
        t.setName(this.prefix + counter.getAndIncrement());

        return t;
    }

    /**
     * Create prefix for thread name.
     */
    public static String threadPrefix(String nodeName, String poolName) {
        return "%" + nodeName + "%" + poolName + "-";
    }

    /**
     * Print uncaught exceptions to log.
     * Default handler of uncaught exceptions for thread pools.
     */
    private static final class LogUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        /** {@inheritDoc} */
        @Override public void uncaughtException(Thread t, Throwable e) {
            LOG.error("Uncaught exception in thread {}", e, t);
        }
    }
}

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

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_THROTTLE_CAPACITY;
import static org.jsr166.ConcurrentLinkedHashMap.DFLT_CONCUR_LVL;

/**
 * Grid log throttle.
 * <p>
 * Errors are logged only if they were not logged for the last
 * {@link #throttleTimeout} milliseconds.
 * Note that not only error messages are checked for duplicates, but also exception
 * classes.
 */
public class GridLogThrottle {
    /** Default throttle timeout in milliseconds (value is <tt>5 * 60 * 1000</tt>). */
    public static final int DFLT_THROTTLE_TIMEOUT = 5 * 60 * 1000;

    /** Throttle timeout in milliseconds. */
    private static volatile int throttleTimeout = DFLT_THROTTLE_TIMEOUT;

    /** Throttle capacity. */
    private static final int throttleCap = IgniteSystemProperties.getInteger(IGNITE_LOG_THROTTLE_CAPACITY, 128);

    /** Errors. */
    private static volatile ConcurrentMap<IgniteBiTuple<Class<? extends Throwable>, String>, Long> errors =
        new ConcurrentLinkedHashMap<>(throttleCap, 0.75f, DFLT_CONCUR_LVL, throttleCap);

    /**
     * Sets system-wide log throttle timeout.
     *
     * @param timeout System-wide log throttle timeout.
     */
    public static void throttleTimeout(int timeout) {
        throttleTimeout = timeout;
    }

    /**
     * Gets system-wide log throttle timeout.
     *
     * @return System-side log throttle timeout.
     */
    public static int throttleTimeout() {
        return throttleTimeout;
    }

    /**
     * Gets system-wide log throttle capacity.
     *
     * @return System-side log throttle capacity.
     */
    public static int throttleCapacity() {
        return throttleCap;
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     */
    public static void error(@Nullable IgniteLogger log, @Nullable Throwable e, String msg) {
        assert !F.isEmpty(msg);

        log(log, e, msg, LogLevel.ERROR, false, false);
    }

    /**
     * Logs error if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     * @param byMsg Errors group by message, not by tuple(error, msg).
     */
    public static void error(@Nullable IgniteLogger log, @Nullable Throwable e, String msg, boolean byMsg) {
        assert !F.isEmpty(msg);

        log(log, e, msg, LogLevel.ERROR, false, byMsg);
    }

    /**
     * Logs warning if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void warn(@Nullable IgniteLogger log, String msg) {
        assert !F.isEmpty(msg);

        log(log, null, msg, LogLevel.WARN, false, false);
    }

    /**
     * Logs warning if needed.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param msg Message.
     * @param quite Print warning anyway.
     * @param byMsg Errors group by message, not by tuple(error, msg).
     */
    public static void warn(@Nullable IgniteLogger log, @Nullable Throwable e, String msg, boolean quite, boolean byMsg) {
        assert !F.isEmpty(msg);

        log(log, e, msg, LogLevel.WARN, quite, byMsg);
    }

    /**
     * Logs warning if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param quiet Print warning anyway.
     */
    public static void warn(@Nullable IgniteLogger log, String msg, boolean quiet) {
        assert !F.isEmpty(msg);

        log(log, null, msg, LogLevel.WARN, quiet, false);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message.
     * @param quiet Print info anyway.
     */
    public static void info(@Nullable IgniteLogger log, String msg, boolean quiet) {
        assert !F.isEmpty(msg);

        log(log, null, msg, LogLevel.INFO, quiet, false);
    }

    /**
     * Logs info if needed.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void info(@Nullable IgniteLogger log, String msg) {
        assert !F.isEmpty(msg);

        info(log, msg, false);
    }

    /**
     * Clears all stored data. This will make throttle to behave like a new one.
     */
    public static void clear() {
        errors = new ConcurrentLinkedHashMap<>(throttleCap, 0.75f, DFLT_CONCUR_LVL, throttleCap);
    }

    /**
     * Logs message if needed using desired level.
     *
     * @param log Logger.
     * @param e Error (optional).
     * @param longMsg Long message (or just message).
     * @param level Level where messages should appear.
     * @param byMsg Errors group by message, not by tuple(error, msg).
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private static void log(@Nullable IgniteLogger log,
        @Nullable Throwable e,
        String longMsg,
        LogLevel level,
        boolean quiet,
        boolean byMsg
    ) {
        assert !F.isEmpty(longMsg);

        IgniteBiTuple<Class<? extends Throwable>, String> tup =
            e != null && !byMsg ? F.<Class<? extends Throwable>, String>t(e.getClass(), e.getMessage()) :
                F.<Class<? extends Throwable>, String>t(null, longMsg);

        while (true) {
            Long loggedTs = errors.get(tup);

            long curTs = U.currentTimeMillis();

            if (loggedTs == null || loggedTs < curTs - throttleTimeout) {
                if (replace(tup, loggedTs, curTs)) {
                    level.doLog(log, longMsg, e, quiet);

                    break;
                }
            }
            else
                // Ignore.
                break;
        }
    }

    /**
     * @param t Log throttle entry.
     * @param oldStamp Old timestamp, possibly {@code null}.
     * @param newStamp New timestamp.
     * @return {@code True} if throttle value was replaced.
     */
    private static boolean replace(IgniteBiTuple<Class<? extends Throwable>, String> t, @Nullable Long oldStamp,
        Long newStamp) {
        assert newStamp != null;

        if (oldStamp == null) {
            Long old = errors.putIfAbsent(t, newStamp);

            return old == null;
        }

        return errors.replace(t, oldStamp, newStamp);
    }

    /** Ensure singleton. */
    protected GridLogThrottle() {
        // No-op.
    }

    /**
     *
     */
    private enum LogLevel {
        /** Error level. */
        ERROR {
            @Override public void doLog(IgniteLogger log, String msg, Throwable e, boolean quiet) {
                if (e != null)
                    U.error(log, msg, e);
                else
                    U.error(log, msg);
            }
        },

        /** Warn level. */
        WARN {
            @Override public void doLog(IgniteLogger log, String msg, Throwable e, boolean quiet) {
                if (quiet)
                    U.quietAndWarn(log, msg, e);
                else
                    U.warn(log, msg, e);
            }
        },

        /** Info level. */
        INFO {
            @Override public void doLog(IgniteLogger log, String msg, Throwable e, boolean quiet) {
                if (quiet)
                    U.quietAndInfo(log, msg);
                else {
                    if (log.isInfoEnabled())
                        log.info(msg);
                }
            }
        };

        /**
         * Performs logging operation.
         *
         * @param log Logger to use.
         * @param msg Long message.
         * @param e Exception to attach to log.
         */
        public abstract void doLog(IgniteLogger log, String msg, Throwable e, boolean quiet);
    }
}

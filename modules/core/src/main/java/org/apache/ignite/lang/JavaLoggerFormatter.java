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

package org.apache.ignite.lang;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.ignite.internal.tostring.S;

/**
 * Formatter for JUL logger.
 */
public class JavaLoggerFormatter extends Formatter {
    /** See {@link Level#OFF}. */
    private static final int SEVERITY_OFF = Integer.MAX_VALUE;

    /** See {@link Level#SEVERE}. */
    private static final int SEVERITY_SEVERE = 1000;

    /** See {@link Level#WARNING}. */
    private static final int SEVERITY_WARNING = 900;

    /** See {@link Level#INFO}. */
    private static final int SEVERITY_INFO = 800;

    /** See {@link Level#CONFIG}. */
    private static final int SEVERITY_CONFIG = 700;

    /** See {@link Level#FINE}. */
    private static final int SEVERITY_FINE = 500;

    /** See {@link Level#FINER}. */
    private static final int SEVERITY_FINER = 400;

    /** See {@link Level#ALL}. */
    private static final int SEVERITY_ALL = Integer.MIN_VALUE;

    /** Ascending order for binary search matching the list of severity constants. */
    private static final int[] LEVEL_VALUES = new int[]{
            SEVERITY_ALL, SEVERITY_FINER,
            SEVERITY_FINE, SEVERITY_CONFIG, SEVERITY_INFO,
            SEVERITY_WARNING, SEVERITY_SEVERE, SEVERITY_OFF
    };

    /** Name for anonymous loggers. */
    public static final String ANONYMOUS_LOGGER_NAME = "UNKNOWN";

    /** Date formatter. */
    private static final ThreadLocal<DateTimeFormatter> DATE_FORMATTER = new ThreadLocal<>() {
        /** {@inheritDoc} */
        @Override
        protected DateTimeFormatter initialValue() {
            return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS Z");
        }
    };

    /** {@inheritDoc} */
    @Override
    public String format(LogRecord record) {
        String threadName = Thread.currentThread().getName();

        String logName = record.getLoggerName();

        if (logName == null) {
            logName = ANONYMOUS_LOGGER_NAME;
        } else if (logName.contains(".")) {
            logName = logName.substring(logName.lastIndexOf('.') + 1);
        }

        String ex = null;

        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();

            record.getThrown().printStackTrace(new PrintWriter(sw));

            String stackTrace = sw.toString();

            ex = "\n" + stackTrace;
        }

        return DATE_FORMATTER.get().format(Instant.ofEpochMilli(record.getMillis()).atZone(ZoneId.systemDefault()))
                + " [" + toLevel(record.getLevel().intValue()) + "]["
                + threadName + "]["
                + logName + "] "
                + formatMessage(record)
                + (ex == null ? "\n" : ex);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JavaLoggerFormatter.class, this);
    }

    /**
     * Convert {@linkplain java.util.logging.Level  java.util.logging levels} to {@linkplain System.Logger.Level System logger levels}.
     *
     * @param severity Severity
     * @return {@link System.Logger.Level} according to {@link java.util.logging.Level} int value.
     * @see System.Logger.Level
     */
    private System.Logger.Level toLevel(int severity) {
        switch (severity) {
            case SEVERITY_ALL:
                return System.Logger.Level.ALL;
            case SEVERITY_FINER:
                return System.Logger.Level.TRACE;
            case SEVERITY_FINE:
            case SEVERITY_CONFIG:
                return System.Logger.Level.DEBUG;
            case SEVERITY_INFO:
                return System.Logger.Level.INFO;
            case SEVERITY_WARNING:
                return System.Logger.Level.WARNING;
            case SEVERITY_SEVERE:
                return System.Logger.Level.ERROR;
            case SEVERITY_OFF:
                return System.Logger.Level.OFF;
            default:
                break;
        }

        // return the nearest Level value >= the given level,
        // for level > SEVERE, return SEVERE and exclude OFF
        int i = Arrays.binarySearch(LEVEL_VALUES, 0, LEVEL_VALUES.length - 2, severity);

        return toLevel(LEVEL_VALUES[i >= 0 ? i : (-i - 1)]);
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.logger.java;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Formatter for JUL logger.
 */
public class JavaLoggerFormatter extends Formatter {
    /** Name for anonymous loggers. */
    public static final String ANONYMOUS_LOGGER_NAME = "UNKNOWN";

    /** */
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        /** {@inheritDoc} */
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("HH:mm:ss,SSS");
        }
    };

    /** {@inheritDoc} */
    @Override public String format(LogRecord record) {
        String threadName = Thread.currentThread().getName();

        String logName = record.getLoggerName();

        if (logName == null)
            logName = ANONYMOUS_LOGGER_NAME;
        else if (logName.contains("."))
            logName = logName.substring(logName.lastIndexOf('.') + 1);

        String ex = null;

        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();

            record.getThrown().printStackTrace(new PrintWriter(sw));

            String stackTrace = sw.toString();

            ex = "\n" + stackTrace;
        }

        return "[" + DATE_FORMATTER.get().format(new Date(record.getMillis())) + "][" +
            record.getLevel() + "][" +
            threadName + "][" +
            logName + "] " +
            formatMessage(record) +
            (ex == null ? "\n" : ex);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JavaLoggerFormatter.class, this);
    }
}

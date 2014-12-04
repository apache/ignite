/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.logger.java;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.text.*;
import java.util.*;
import java.util.logging.Formatter;
import java.util.logging.*;

/**
 * Formatter for JUL logger.
 */
public class GridJavaLoggerFormatter extends Formatter {
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
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
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
            record.getMessage() +
            (ex == null ? "\n" : ex);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJavaLoggerFormatter.class, this);
    }
}

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

package org.apache.ignite.testframework;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Logger which logs to string buffer.
 */
public class GridStringLogger implements IgniteLogger {
    /** Initial string builder capacity in bytes */
    private static final int INITIAL = 1024 * 33;

    /** Maximum characters to be kept in string builder */
    private static final int CHAR_CNT = 1024 * 32;

    /** Builder to accumulate log messages */
    private StringBuilder buf = new StringBuilder(INITIAL);

    /** */
    private final boolean dbg;

    /** */
    private volatile int chars = CHAR_CNT;

    /** */
    private final IgniteLogger echo;

    /**
     *
     */
    public GridStringLogger() {
        this(false);
    }

    /**
     * @param dbg Debug flag.
     */
    public GridStringLogger(boolean dbg) {
        this(dbg, null);
    }

    /**
     * @param dbg Debug flag.
     * @param echo Logger to echo all messages.
     */
    public GridStringLogger(boolean dbg, @Nullable IgniteLogger echo) {
        this.dbg = dbg;
        this.echo = echo;
    }

    /**
     * @param chars History buffer length.
     */
    public void logLength(int chars) {
        this.chars = chars;
    }

    /**
     * @return History buffer length.
     */
    private int logLength() {
        return chars;
    }

    /**
     * @param msg Message to log.
     */
    private synchronized void log(String msg) {
        buf.append(msg).append(U.nl());

        if (echo != null)
            echo.info("[GridStringLogger echo] " + msg);

        int logLength = logLength();

        if (buf.length() > logLength) {
            if (echo != null)
                echo.warning("Cleaning GridStringLogger history.");

            buf.delete(0, buf.length() - logLength);
        }

        assert buf.length() <= logLength;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public synchronized void warning(String msg, @Nullable Throwable e) {
        log(msg);

        if (e != null)
            logThrowable(e);
    }

    /**
     * @param e Exception.
     */
    private void logThrowable(@NotNull Throwable e) {
        StringWriter writer = new StringWriter();

        e.printStackTrace(new PrintWriter(writer));

        log(writer.toString());
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public synchronized void error(String msg, @Nullable Throwable e) {
        log(msg);

        if (e != null)
            logThrowable(e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return dbg;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return dbg;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }

    /**
     * Resets logger.
     */
    public synchronized void reset() {
        buf.setLength(0);
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return buf.toString();
    }
}
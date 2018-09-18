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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that perform any actions when certain message is logged.
 */
public class GridEventLogger implements IgniteLogger {
    /** */
    private IgniteLogger echo;

    /** */
    private ConcurrentMap<Pattern, CI1<String>> lsnrs = new ConcurrentHashMap<>();

    /** */
    private final boolean dbg;

    /**
     * Default constructor.
     */
    public GridEventLogger() {
        this(false);
    }

    /**
     * @param dbg Debug flag.
     */
    public GridEventLogger(boolean dbg) {
        this(dbg, null);
    }

    /**
     * @param echo Logger to echo all messages.
     */
    public GridEventLogger(IgniteLogger echo) {
        this(false, echo);
    }

    /**
     * @param dbg Debug flag.
     * @param echo Logger to echo all messages.
     */
    public GridEventLogger(boolean dbg, @Nullable IgniteLogger echo) {
        this.dbg = dbg;
        this.echo = echo;
    }

    /**
     * Register log message listener.
     *
     * @param regex Regular expression matched against log messages.
     * @param lsnr Listener to execute.
     * @throws PatternSyntaxException If the expression's syntax is invalid.
     */
    public void listen(@NotNull String regex, @NotNull CI1<String> lsnr) {
        lsnrs.put(Pattern.compile(regex), lsnr);
    }

    /**
     * Clears all listeners.
     */
    public void reset() {
        lsnrs.clear();
    }

    /** {@inheritDoc} */
    @Override public GridEventLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!dbg)
            return;

        if (echo != null)
            echo.trace(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!dbg)
            return;

        if (echo != null)
            echo.debug(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (echo != null)
            echo.info(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable t) {
        if (echo != null)
            echo.warning(msg, t);

        applyListeners(msg);

        if (t != null)
            applyListeners(X.getFullStackTrace(t));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable t) {
        if (echo != null)
            echo.error(msg, t);

        applyListeners(msg);

        if (t != null)
            applyListeners(X.getFullStackTrace(t));
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
    @Override public String fileName() {
        return null;
    }

    /**
     * Apply listeners which regular expression matches message.
     *
     * @param msg Message to check.
     */
    private void applyListeners(String msg) {
        if (F.isEmpty(msg))
            return;

        for (Map.Entry<Pattern, CI1<String>> entry : lsnrs.entrySet()) {
            if (entry.getKey().matcher(msg).find())
                entry.getValue().apply(msg);
        }
    }
}

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

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that performs any actions when certain message is logged.
 * It can be useful in tests to ensure that a specific message was (or was not) printed to the log.
 */
public class ListeningTestLogger implements IgniteLogger {
    /**
     * Logger to echo all messages, limited by {@code dbg} flag.
     */
    private final IgniteLogger echo;

    /**
     * Registered log messages listeners.
     */
    private final Collection<Consumer<String>> lsnrs = new CopyOnWriteArraySet<>();

    /**
     * Default constructor.
     */
    public ListeningTestLogger() {
        this(null);
    }

    /**
     * @param dbg Ignored. For setting debug use {@link GridAbstractTest#setRootLoggerDebugLevel()}.
     * @deprecated Use {@link #ListeningTestLogger()} instead.
     */
    @Deprecated
    public ListeningTestLogger(boolean dbg) {
        this(null);
    }

    /**
     * @param dbg Ignored. For setting debug use {@link GridAbstractTest#setRootLoggerDebugLevel()}.
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     * @deprecated Use {@link #ListeningTestLogger(IgniteLogger)} instead.
     */
    @Deprecated
    public ListeningTestLogger(boolean dbg, @Nullable IgniteLogger echo) {
        this(echo);
    }

    /**
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     */
    public ListeningTestLogger(@Nullable IgniteLogger echo) {
        this.echo = echo;
    }

    /**
     * @param dbg Ignored. For setting debug use {@link GridAbstractTest#setRootLoggerDebugLevel()}.
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     * @param lsnrs LogListeners to register instantly.
     * @deprecated Use {@link #ListeningTestLogger(IgniteLogger, LogListener...)} instead.
     */
    @Deprecated
    public ListeningTestLogger(boolean dbg, @Nullable IgniteLogger echo, @NotNull LogListener... lsnrs) {
        this(echo, lsnrs);
    }

    /**
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     * @param lsnrs LogListeners to register instantly.
     */
    public ListeningTestLogger(@Nullable IgniteLogger echo, @NotNull LogListener... lsnrs) {
        this(echo);

        for (LogListener lsnr : lsnrs)
            registerListener(lsnr);
    }

    /**
     * Registers message listener.
     *
     * @param lsnr Message listener.
     */
    public void registerListener(@NotNull LogListener lsnr) {
        lsnr.reset();

        lsnrs.add(lsnr);
    }

    /**
     * Resets and registers all message listeners.
     *
     * @param lsnrs Message listeners.
     */
    public void registerAllListeners(@NotNull LogListener... lsnrs) {
        for (LogListener lsnr : lsnrs) {
            lsnr.reset();

            this.lsnrs.add(lsnr);
        }
    }

    /**
     * Registers message listener.
     * <p>
     * NOTE listener is executed in the thread causing the logging, so it is not recommended to throw any exceptions
     * from it. Use {@link LogListener} to create message predicates with assertions.
     *
     * @param lsnr Message listener.
     * @see LogListener
     */
    public void registerListener(@NotNull Consumer<String> lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * Unregisters message listener.
     *
     * @param lsnr Message listener.
     */
    public void unregisterListener(@NotNull Consumer<String> lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * Clears all listeners.
     */
    public void clearListeners() {
        lsnrs.clear();
    }

    /** {@inheritDoc} */
    @Override public ListeningTestLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (echo != null)
            echo.trace(msg);

        applyListeners(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
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
        return echo != null && echo.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return echo != null && echo.isDebugEnabled();
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
     * Applies listeners whose pattern is found in the message.
     *
     * @param msg Message to check.
     */
    private void applyListeners(String msg) {
        if (msg == null)
            return;

        for (Consumer<String> lsnr : lsnrs)
            lsnr.accept(msg);
    }
}

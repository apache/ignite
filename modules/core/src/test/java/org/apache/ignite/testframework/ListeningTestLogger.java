/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testframework;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that performs any actions when certain message is logged.
 * It can be useful in tests to ensure that a specific message was (or was not) printed to the log.
 */
public class ListeningTestLogger implements IgniteLogger {
    /**
     * If set to {@code true}, enables debug and trace log messages processing.
     */
    private final boolean dbg;

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
        this(false);
    }

    /**
     * @param dbg If set to {@code true}, enables debug and trace log messages processing.
     */
    public ListeningTestLogger(boolean dbg) {
        this(dbg, null);
    }

    /**
     * @param dbg If set to {@code true}, enables debug and trace log messages processing.
     * @param echo Logger to echo all messages, limited by {@code dbg} flag.
     */
    public ListeningTestLogger(boolean dbg, @Nullable IgniteLogger echo) {
        this.dbg = dbg;
        this.echo = echo;
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

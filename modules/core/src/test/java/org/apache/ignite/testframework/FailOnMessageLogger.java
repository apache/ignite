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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Used for log monitoring in order to track down any undesirable exception. This implementation stores
 * only the last message at which undesirable exception was detected.
 */
public class FailOnMessageLogger implements IgniteLogger {

    /** */
    private final String checkMsg;

    /** */
    private final boolean checkDbgTrc;

    /** */
    private final IgniteLogger echo;

    /** Stores message in which {@code checkMsg} was found. */
    private volatile String failOnMessage;

    /**
     * @param checkDbgTrc Debug flag.
     * @param echo Logger to echo all messages.
     */
    public FailOnMessageLogger(String checkMsg, boolean checkDbgTrc, @Nullable IgniteLogger echo) {
        this.checkDbgTrc = checkDbgTrc;
        this.echo = echo;
        this.checkMsg = checkMsg;
    }

    /**
     * @param msg Message to log.
     */
    private void log(String msg) {
        if (echo != null)
            echo.info("[FailOnMessageLogger echo] " + msg);

        if (msg != null && msg.contains(checkMsg)) {
            failOnMessage = msg;
            System.err.print(msg);
            System.err.flush();
            assert false : msg;
        }
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
    @Override public void warning(String msg, @Nullable Throwable e) {
        log(msg);

        if (e != null)
            log(e.toString());
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        log(msg);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        log(msg);

        if (e != null)
            log(msg + e.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return checkDbgTrc;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return checkDbgTrc;
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
     * @return String on which {@code checkMsg} was detected. Null if everything is ok.
     */
    @Nullable public String getFailOnMessage() {
        return failOnMessage;
    }

    /**
     * Clears string on which {@code checkMsg} was detected.
     */
    public void reset() {
        failOnMessage = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FailOnMessageLogger.class, this);
    }
}

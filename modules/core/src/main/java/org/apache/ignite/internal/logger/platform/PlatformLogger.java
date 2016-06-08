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

package org.apache.ignite.internal.logger.platform;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Logger that delegates to platform.
 */
public class PlatformLogger implements IgniteLogger {
    /** Callbacks. */
    private final PlatformCallbackGateway gate;

    /** Quiet flag. */
    private final Boolean quiet;

    /** */
    private static final byte LVL_TRACE = 0;

    /** */
    private static final byte LVL_DEBUG = 1;

    /** */
    private static final byte LVL_INFO = 2;

    /** */
    private static final byte LVL_WARN = 3;

    /** */
    private static final byte LVL_ERROR = 4;

    /** */
    private final String category;

    /**
     * Ctor.
     *
     * @param gate Callback gateway.
     */
    public PlatformLogger(PlatformCallbackGateway gate, Object ctgr) {
        this.gate = gate;

        // Default quiet to false so that Java does not write anything to console.
        // Platform is responsible for console output, we do not want to mix these.
        quiet = Boolean.valueOf(System.getProperty(IGNITE_QUIET, "false"));

        category = ctgr instanceof Class ? ((Class)ctgr).getName() : String.valueOf(ctgr);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new PlatformLogger(gate, ctgr);
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        log(LVL_TRACE, msg, null);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        log(LVL_DEBUG, msg, null);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        log(LVL_INFO, msg, null);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        log(LVL_WARN, msg, null);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        log(LVL_WARN, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        log(LVL_ERROR, msg, null);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        log(LVL_ERROR, msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return isLevelEnabled(LVL_TRACE);
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return isLevelEnabled(LVL_DEBUG);
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return isLevelEnabled(LVL_INFO);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        // TODO: Do we need this on platform side? Not sure.
        return quiet;
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return null;
    }

    /**
     * Returns a value indicating whether specified log level is enabled.
     *
     * @param level Log level.
     * @return Whether specified log level is enabled.
     */
    private boolean isLevelEnabled(byte level) {
        // TODO: native
        // TODO: This is going to be called a lot!
        return true;
    }

    private void log(byte level, String msg, @Nullable Throwable e) {
        // TODO: native
        // TODO: Unwrap platform error if possible
        // TODO: pass category

        /*if (level > 1)
            System.out.printf("%s: %s\n", cat, msg);*/
    }
}

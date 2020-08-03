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
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Logger that delegates to platform.
 */
public class PlatformLogger implements IgniteLogger {
    /** */
    public static final int LVL_TRACE = 0;

    /** */
    public static final int LVL_DEBUG = 1;

    /** */
    public static final int LVL_INFO = 2;

    /** */
    public static final int LVL_WARN = 3;

    /** */
    public static final int LVL_ERROR = 4;

    /** Callbacks. */
    @GridToStringExclude
    private volatile PlatformCallbackGateway gate;

    /** Context. */
    @GridToStringExclude
    private volatile PlatformContext ctx;

    /** Category. */
    @GridToStringExclude
    private final String category;

    /** Trace flag. */
    @GridToStringInclude
    private volatile boolean traceEnabled;

    /** Debug flag. */
    @GridToStringInclude
    private volatile boolean debugEnabled;

    /** Info flag. */
    @GridToStringInclude
    private volatile boolean infoEnabled;

    /** Quiet flag. */
    @GridToStringInclude
    private final boolean isQuiet = Boolean.valueOf(System.getProperty(IGNITE_QUIET, "true"));

    /**
     * Ctor.
     *
     */
    public PlatformLogger() {
        category = null;
    }

    /**
     * Ctor.
     */
    private PlatformLogger(PlatformCallbackGateway gate, PlatformContext ctx, String category,
        boolean traceEnabled, boolean debugEnabled, boolean infoEnabled) {
        this.gate = gate;
        this.ctx = ctx;
        this.category = category;
        this.traceEnabled = traceEnabled;
        this.debugEnabled = debugEnabled;
        this.infoEnabled = infoEnabled;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new PlatformLogger(gate, ctx, getCategoryString(ctgr), traceEnabled, debugEnabled, infoEnabled);
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
        return traceEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return debugEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return infoEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return isQuiet;
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return null;
    }

    /**
     * Sets the gateway.
     *
     * @param gate Callback gateway.
     */
    public void setGateway(PlatformCallbackGateway gate) {
        assert gate != null;
        this.gate = gate;

        // Pre-calculate enabled levels (JNI calls are expensive)
        traceEnabled = gate.loggerIsLevelEnabled(LVL_TRACE);
        debugEnabled = gate.loggerIsLevelEnabled(LVL_DEBUG);
        infoEnabled = gate.loggerIsLevelEnabled(LVL_INFO);
    }

    /**
     * Sets the context.
     *
     * @param ctx Platform context.
     */
    public void setContext(PlatformContext ctx) {
        assert ctx != null;
        this.ctx = ctx;
    }

    /**
     * Logs the message.
     *
     * @param level Log level.
     * @param msg Message.
     * @param e Exception.
     */
    private void log(int level, String msg, @Nullable Throwable e) {
        String errorInfo = null;

        if (e != null)
            errorInfo = X.getFullStackTrace(e);

        PlatformNativeException e0 = X.cause(e, PlatformNativeException.class);
        if (ctx != null && e0 != null) {
            try (PlatformMemory mem = ctx.memory().allocate()) {
                PlatformOutputStream out = mem.output();
                BinaryRawWriterEx writer = ctx.writer(out);
                writer.writeObject(e0.cause());
                out.synchronize();

                gate.loggerLog(level, msg, category, errorInfo, mem.pointer());
            }
        }
        else {
            gate.loggerLog(level, msg, category, errorInfo, 0);
        }
    }

    /**
     * Gets the category string.
     *
     * @param ctgr Category object.
     * @return Category string.
     */
    private static String getCategoryString(Object ctgr) {
        return ctgr instanceof Class
            ? ((Class)ctgr).getName()
            : (ctgr == null ? null : String.valueOf(ctgr));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformLogger.class, this);
    }
}

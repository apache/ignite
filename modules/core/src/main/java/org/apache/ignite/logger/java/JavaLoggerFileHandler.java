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

package org.apache.ignite.logger.java;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * File logging handler which skips all the messages until node ID is set.
 */
public final class JavaLoggerFileHandler extends StreamHandler {
    /** Log manager. */
    private static final LogManager manager = LogManager.getLogManager();

    /** Handler delegate. */
    private volatile FileHandler delegate;

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void publish(LogRecord record) {
        FileHandler delegate0 = delegate;

        if (delegate0 != null)
            delegate0.publish(record);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void flush() {
        FileHandler delegate0 = delegate;

        if (delegate0 != null)
            delegate0.flush();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void close() throws SecurityException {
        FileHandler delegate0 = delegate;

        if (delegate0 != null)
            delegate0.close();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public boolean isLoggable(LogRecord record) {
        FileHandler delegate0 = delegate;

        return delegate0 != null && delegate0.isLoggable(record);
    }

    /**
     * Sets Node id and instantiates {@link FileHandler} delegate.
     *
     * @param nodeId Node id.
     */
    public void nodeId(UUID nodeId) throws IgniteCheckedException, IOException {
        if (delegate != null)
            return;

        String clsName = getClass().getName();

        String ptrn = manager.getProperty(clsName + ".pattern");

        if (ptrn == null)
            ptrn = "ignite-%{id8}.%g.log";

        ptrn = new File(logDirectory(), ptrn.replace("%{id8}", U.id8(nodeId))).getAbsolutePath();

        int limit = getIntProperty(clsName + ".limit", 0);

        if (limit < 0)
            limit = 0;

        int cnt = getIntProperty(clsName + ".count", 1);

        if (cnt <= 0)
            cnt = 1;

        boolean append = getBooleanProperty(clsName + ".append", false);

        FileHandler delegate0;

        synchronized (this) {
            if (delegate != null)
                return;

            delegate = delegate0 = new FileHandler(ptrn, limit, cnt, append);
        }

        delegate0.setLevel(getLevel());
        delegate0.setFormatter(getFormatter());
        delegate0.setEncoding(getEncoding());
        delegate0.setFilter(getFilter());
        delegate0.setErrorManager(getErrorManager());
    }

    /**
     * Returns current log file.
     *
     * @return Pattern or {@code null} if node id has not been set yet.
     */
    @Nullable public String fileName() {
        return JavaLogger.fileName(delegate);
    }

    /**
     * Resolves logging directory.
     *
     * @return Logging directory.
     */
    private static File logDirectory() throws IgniteCheckedException {
        return !F.isEmpty(U.IGNITE_LOG_DIR) ? new File(U.IGNITE_LOG_DIR) : U.resolveWorkDirectory("log", false);
    }

    /**
     * Returns integer property from logging configuration.
     *
     * @param name Property name.
     * @param dfltVal Default value.
     * @return Parsed property value if it is set and valid or default value otherwise.
     */
    private int getIntProperty(String name, int dfltVal) {
        String val = manager.getProperty(name);

        if (val == null)
            return dfltVal;

        try {
            return Integer.parseInt(val.trim());
        }
        catch (Exception ex) {
            ex.printStackTrace();

            return dfltVal;
        }
    }

    /**
     * Returns boolean property from logging configuration.
     *
     * @param name Property name.
     * @param dfltVal Default value.
     * @return Parsed property value if it is set and valid or default value otherwise.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    private boolean getBooleanProperty(String name, boolean dfltVal) {
        String val = manager.getProperty(name);

        if (val == null)
            return dfltVal;

        val = val.toLowerCase();

        if ("true".equals(val) || "1".equals(val))
            return true;

        if ("false".equals(val) || "0".equals(val))
            return false;

        return dfltVal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JavaLoggerFileHandler.class, this);
    }
}
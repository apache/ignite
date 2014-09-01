/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.java;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/**
 * File logging handler which skips all the messages until node ID is set.
 */
public final class GridJavaLoggerFileHandler extends StreamHandler {
    /* GridGain Logging Directory. */
    public static final String GRIDGAIN_LOG_DIR = System.getenv("GRIDGAIN_LOG_DIR");

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
    public void nodeId(UUID nodeId) throws GridException, IOException {
        if (delegate != null)
            return;

        String className = getClass().getName();

        String pattern = manager.getProperty(className + ".pattern");

        if (pattern == null)
            pattern = "gridgain-%{id8}.%g.log";

        pattern = new File(logDirectory(), pattern.replace("%{id8}", U.id8(nodeId))).getAbsolutePath();

        int limit = getIntProperty(className + ".limit", 0);

        if (limit < 0)
            limit = 0;

        int count = getIntProperty(className + ".count", 1);

        if (count <= 0)
            count = 1;

        boolean append = getBooleanProperty(className + ".append", false);

        FileHandler delegate0;

        synchronized (this) {
            if (delegate != null)
                return;

            delegate = delegate0 = new FileHandler(pattern, limit, count, append);
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
        return GridJavaLogger.fileName(delegate);
    }

    /**
     * Resolves logging directory.
     *
     * @return Logging directory.
     */
    private static File logDirectory() throws GridException {
        return GRIDGAIN_LOG_DIR != null ? new File(GRIDGAIN_LOG_DIR) : U.resolveWorkDirectory("log", false);
    }

    /**
     * Returns integer property from logging configuration.
     *
     * @param name Property name.
     * @param defaultValue Default value.
     * @return Parsed property value if it is set and valid or default value otherwise.
     */
    private int getIntProperty(String name, int defaultValue) {
        String val = manager.getProperty(name);

        if (val == null)
            return defaultValue;

        try {
            return Integer.parseInt(val.trim());
        }
        catch (Exception ex) {
            ex.printStackTrace();

            return defaultValue;
        }
    }

    /**
     * Returns boolean property from logging configuration.
     *
     * @param name Property name.
     * @param defaultValue Default value.
     * @return Parsed property value if it is set and valid or default value otherwise.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    private boolean getBooleanProperty(String name, boolean defaultValue) {
        String val = manager.getProperty(name);

        if (val == null)
            return defaultValue;

        val = val.toLowerCase();

        if ("true".equals(val) || "1".equals(val))
            return true;

        if ("false".equals(val) || "0".equals(val))
            return false;

        return defaultValue;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJavaLoggerFileHandler.class, this);
    }
}

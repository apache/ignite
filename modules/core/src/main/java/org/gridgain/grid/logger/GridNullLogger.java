/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger;

import org.jetbrains.annotations.*;

/**
 * Logger which does not output anything.
 */
public class GridNullLogger implements GridLogger {
    /** {@inheritDoc} */
    @Override public GridLogger getLogger(Object ctgr) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }
}

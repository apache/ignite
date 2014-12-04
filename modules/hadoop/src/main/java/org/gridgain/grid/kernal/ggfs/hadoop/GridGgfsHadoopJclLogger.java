/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.commons.logging.*;
import org.gridgain.grid.logger.*;
import org.jetbrains.annotations.*;

/**
 * JCL logger wrapper for Hadoop.
 */
public class GridGgfsHadoopJclLogger implements IgniteLogger {
    /** */
    private static final long serialVersionUID = 0L;

    /** JCL implementation proxy. */
    private Log impl;

    /**
     * Constructor.
     *
     * @param impl JCL implementation to use.
     */
    GridGgfsHadoopJclLogger(Log impl) {
        assert impl != null;

        this.impl = impl;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new GridGgfsHadoopJclLogger(LogFactory.getLog(
            ctgr instanceof Class ? ((Class)ctgr).getName() : String.valueOf(ctgr)));
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        impl.trace(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        impl.debug(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        impl.info(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warn(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        impl.warn(msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.error(msg);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return !isInfoEnabled() && !isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.error(msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridGgfsHadoopJclLogger [impl=" + impl + ']';
    }
}

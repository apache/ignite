/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.tomcat;

import org.apache.juli.logging.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.startup.tomcat.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

/**
 * Logger to use in Tomcat loaders. Implementation simply delegates to
 * <a target=_new href="http://tomcat.apache.org/tomcat-6.0-doc/logging.html">Tomcat</a> logging.
 * <p>
 * It's recommended to use GridGain logger injection instead of using/instantiating
 * logger in your task/job code. See {@link GridLoggerResource} annotation about logger
 * injection.
 * @see GridTomcatStartup
 */
public class GridTomcatLogger extends GridMetadataAwareAdapter implements GridLogger {
    /** */
    private static final long serialVersionUID = 0L;

    /** Implementation proxy. */
    private Log impl;

    /**
     * Creates new logger with given implementation.
     */
    public GridTomcatLogger() {
        this(LogFactory.getLog("root"));
    }

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Implementation to use.
     */
    public GridTomcatLogger(Log impl) {
        assert impl != null;

        this.impl = impl;
    }

    /** {@inheritDoc} */
    @Override public GridTomcatLogger getLogger(Object ctgr) {
        return new GridTomcatLogger(LogFactory.getLog(ctgr.toString()));
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!impl.isTraceEnabled())
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

        impl.trace(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!impl.isDebugEnabled())
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        impl.debug(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (!impl.isInfoEnabled())
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

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
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.error(msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return !isInfoEnabled() && !isDebugEnabled();
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
}


/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.java;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.logging.*;

/**
 * Logger to use with Java logging. Implementation simply delegates to Java Logging.
 * <p>
 * Here is an example of configuring Java logger in GridGain configuration Spring
 * file to work over log4j implementation. Note that we use the same configuration file
 * as we provide by default:
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.gridgain.grid.logger.java.GridJavaLogger"&gt;
 *              &lt;constructor-arg type="java.util.logging.Logger"&gt;
 *                  &lt;bean class="java.util.logging.Logger"&gt;
 *                      &lt;constructor-arg type="java.lang.String" value="global"/&gt;
 *                  &lt;/bean&gt;
 *              &lt;/constructor-arg&gt;
 *          &lt;/bean&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * or
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.gridgain.grid.logger.java.GridJavaLogger"/&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * And the same configuration if you'd like to configure GridGain in your code:
 * <pre name="code" class="java">
 *      GridConfiguration cfg = new GridConfiguration();
 *      ...
 *      GridLogger log = new GridJavaLogger(Logger.global);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or which is actually the same:
 * <pre name="code" class="java">
 *      GridConfiguration cfg = new GridConfiguration();
 *      ...
 *      GridLogger log = new GridJavaLogger();
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * Please take a look at <a target=_new href="http://java.sun.com/j2se/1.4.2/docs/api20/java/util/logging/Logger.html>Logger javadoc</a>
 * for additional information.
 * <p>
 * It's recommended to use GridGain logger injection instead of using/instantiating
 * logger in your task/job code. See {@link GridLoggerResource} annotation about logger
 * injection.
 */
public class GridJavaLogger extends GridMetadataAwareAdapter implements GridLogger {
    /** */
    private static final long serialVersionUID = 0L;

    /** Java Logging implementation proxy. */
    private Logger impl;

    /**
     * Creates new logger.
     */
    public GridJavaLogger() {
        this(Logger.getLogger(Logger.GLOBAL_LOGGER_NAME));
    }

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Java Logging implementation to use.
     */
    public GridJavaLogger(Logger impl) {
        assert impl != null;

        this.impl = impl;
    }

    /** {@inheritDoc} */
    @Override public GridLogger getLogger(Object ctgr) {
        return new GridJavaLogger(Logger.getLogger(
            ctgr instanceof Class ? ((Class)ctgr).getName() : String.valueOf(ctgr)));
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!impl.isLoggable(Level.FINEST))
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

        impl.finest(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!impl.isLoggable(Level.FINE))
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        impl.fine(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (!impl.isLoggable(Level.INFO))
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

        impl.info(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warning(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        impl.log(Level.WARNING, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.warning(msg);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return !isInfoEnabled() && !isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.log(Level.WARNING, msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isLoggable(Level.FINEST);
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isLoggable(Level.FINE);
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isLoggable(Level.INFO);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return null;
    }
}

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

package org.apache.ignite.logger.log4j2;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.AppenderControl;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSOLE_APPENDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Log4j2-based implementation for logging. This logger should be used
 * by loaders that have prefer <a target=_new href="http://logging.apache.org/log4j/2.x/index.html">log4j2</a>-based logging.
 * <p>
 * Here is a typical example of configuring log4j2 logger in Ignite configuration file:
 * <pre name="code" class="xml">
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.log4j2.Log4J2Logger"&gt;
 *              &lt;constructor-arg type="java.lang.String" value="config/ignite-log4j2.xml"/&gt;
 *          &lt;/bean>
 *      &lt;/property&gt;
 * </pre>
 * and from your code:
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      URL xml = U.resolveIgniteUrl("config/custom-log4j2.xml");
 *      IgniteLogger log = new Log4J2Logger(xml);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 *
 * Please take a look at <a target=_new href="http://logging.apache.org/log4j/2.x/index.html">Apache Log4j 2</a>
 * for additional information.
 * <p>
 * It's recommended to use Ignite logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.LoggerResource} annotation about logger
 * injection.
 */
public class Log4J2Logger implements IgniteLogger, LoggerNodeIdAware {
    /** */
    private static final String NODE_ID = "nodeId";

    /** */
    private static final String CONSOLE_APPENDER = "autoConfiguredIgniteConsoleAppender";

    /** */
    private static volatile boolean inited;

    /** */
    private static volatile boolean quiet0;

    /** */
    private static final Object mux = new Object();

    /** Logger implementation. */
    @GridToStringExclude
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private Logger impl;

    /** Path to configuration file. */
    @GridToStringExclude
    private final String cfg;

    /** Quiet flag. */
    private final boolean quiet;

    /** Node ID. */
    @GridToStringExclude
    private volatile UUID nodeId;

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Log4j implementation to use.
     */
    private Log4J2Logger(final Logger impl, String path) {
        assert impl != null;
        
        addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                return impl;
            }
        });

        quiet = quiet0;
        cfg = path;
    }

    /**
     * Creates new logger with given configuration {@code path}.
     *
     * @param path Path to log4j2 configuration XML file.
     * @throws IgniteCheckedException Thrown in case logger can't be created.
     */
    public Log4J2Logger(String path) throws IgniteCheckedException {
        if (path == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j2 must be specified.");

        final URL cfgUrl = U.resolveIgniteUrl(path);

        if (cfgUrl == null)
            throw new IgniteCheckedException("Log4j2 configuration path was not found: " + path);

        addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LogManager.ROOT_LOGGER_NAME, cfgUrl.toString());

                return (Logger)LogManager.getRootLogger();
            }
        });

        quiet = quiet0;
        cfg = path;
    }

    /**
     * Creates new logger with given configuration {@code cfgFile}.
     *
     * @param cfgFile Log4j configuration XML file.
     * @throws IgniteCheckedException Thrown in case logger can't be created.
     */
    public Log4J2Logger(File cfgFile) throws IgniteCheckedException {
        if (cfgFile == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j must be specified.");

        if (!cfgFile.exists() || cfgFile.isDirectory())
            throw new IgniteCheckedException("Log4j2 configuration path was not found or is a directory: " + cfgFile);

        final String path = cfgFile.getAbsolutePath();

        addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LogManager.ROOT_LOGGER_NAME, path);

                return (Logger)LogManager.getRootLogger();
            }
        });

        quiet = quiet0;
        cfg = cfgFile.getPath();
    }

    /**
     * Creates new logger with given configuration {@code cfgUrl}.
     *
     * @param cfgUrl URL for Log4j configuration XML file.
     * @throws IgniteCheckedException Thrown in case logger can't be created.
     */
    public Log4J2Logger(final URL cfgUrl) throws IgniteCheckedException {
        if (cfgUrl == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j must be specified.");

        addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LogManager.ROOT_LOGGER_NAME, cfgUrl.toString());

                return (Logger)LogManager.getRootLogger();
            }
        });

        quiet = quiet0;
        cfg = cfgUrl.getPath();
    }

    /**
     * Cleans up the logger configuration. Should be used in unit tests only for sequential tests run with
     * different configurations
     */
    static void cleanup() {
        synchronized (mux) {
            if (inited)
                LogManager.shutdown();

            inited = false;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        for (Logger log = impl; log != null; log = log.getParent()) {
            for (Appender a : log.getAppenders().values()) {
                if (a instanceof FileAppender)
                    return ((FileAppender)a).getFileName();

                if (a instanceof RollingFileAppender)
                    return ((RollingFileAppender)a).getFileName();

                if (a instanceof RoutingAppender) {
                    try {
                        RoutingAppender routing = (RoutingAppender)a;

                        Field appsFiled = routing.getClass().getDeclaredField("appenders");

                        appsFiled.setAccessible(true);

                        Map<String, AppenderControl> appenders = (Map<String, AppenderControl>)appsFiled.get(routing);

                        for (AppenderControl control : appenders.values()) {
                            Appender innerApp = control.getAppender();

                            if (innerApp instanceof FileAppender)
                                return normalize(((FileAppender)innerApp).getFileName());

                            if (innerApp instanceof RollingFileAppender)
                                return normalize(((RollingFileAppender)innerApp).getFileName());
                        }
                    }
                    catch (IllegalAccessException | NoSuchFieldException e) {
                        error("Failed to get file name (was the implementation of log4j2 changed?).", e);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Normalizes given path for windows.
     * Log4j2 doesn't replace unix directory delimiters which used at 'fileName' to windows.
     *
     * @param path Path.
     * @return Normalized path.
     */
    private String normalize(String path) {
        if (!U.isWindows())
            return path;

        return path.replace('/', File.separatorChar);
    }

    /**
     * Adds console appender when needed with some default logging settings.
     *
     * @param initLogClo Optional log implementation init closure.
     */
    private void addConsoleAppenderIfNeeded(@Nullable IgniteClosure<Boolean, Logger> initLogClo) {
        if (inited) {
            // Do not init.
            impl = initLogClo.apply(false);

            return;
        }

        synchronized (mux) {
            if (inited) {
                // Do not init.
                impl = initLogClo.apply(false);

                return;
            }

            // Init logger impl.
            impl = initLogClo.apply(true);

            boolean quiet = Boolean.valueOf(System.getProperty(IGNITE_QUIET, "true"));

            boolean consoleAppenderFound = false;
            Logger rootLogger = null;

            for (Logger log = impl; log != null; ) {
                if (!consoleAppenderFound) {
                    for (Appender appender : log.getAppenders().values()) {
                        if (appender instanceof ConsoleAppender) {
                            if ("CONSOLE_ERR".equals(appender.getName()))
                                continue;

                            consoleAppenderFound = true;

                            break;
                        }
                    }
                }

                if (log.getParent() == null) {
                    rootLogger = log;

                    break;
                }
                else
                    log = log.getParent();
            }

            if (consoleAppenderFound && quiet)
                // User configured console appender, but log is quiet.
                quiet = false;

            if (!consoleAppenderFound && !quiet &&
                Boolean.valueOf(System.getProperty(IGNITE_CONSOLE_APPENDER, "true"))) {
                // Console appender not found => we've looked through all categories up to root.
                assert rootLogger != null;

                // User launched ignite in verbose mode and did not add console appender with INFO level
                // to configuration and did not set IGNITE_CONSOLE_APPENDER to false.
                createConsoleLogger();
            }

            quiet0 = quiet;
            inited = true;
        }
    }

    /**
     * Creates console appender with some reasonable default logging settings.
     *
     * @return Logger with auto configured console appender.
     */
    public Logger createConsoleLogger() {
        // from http://logging.apache.org/log4j/2.x/manual/customconfig.html
        final LoggerContext ctx = impl.getContext();

        final Configuration cfg = ctx.getConfiguration();

        PatternLayout.Builder builder = PatternLayout.newBuilder()
            .withPattern("%d{ISO8601}][%-5p][%t][%c{1}] %m%n")
            .withCharset(Charset.defaultCharset())
            .withAlwaysWriteExceptions(false)
            .withNoConsoleNoAnsi(false);

        PatternLayout layout = builder.build();

        ConsoleAppender.Builder consoleAppenderBuilder = ConsoleAppender.newBuilder()
            .withName(CONSOLE_APPENDER)
            .withLayout(layout);

        ConsoleAppender consoleApp = consoleAppenderBuilder.build();

        consoleApp.start();

        cfg.addAppender(consoleApp);
        cfg.getRootLogger().addAppender(consoleApp, Level.TRACE, null);

        ctx.updateLoggers(cfg);

        return ctx.getRootLogger();
    }

    /** {@inheritDoc} */
    @Override public void setNodeId(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        this.nodeId = nodeId;

        // Set nodeId as system variable to be used at configuration.
        System.setProperty(NODE_ID, U.id8(nodeId));

        if (inited) {
            final LoggerContext ctx = impl.getContext();

            synchronized (mux) {
                inited = false;
            }

            addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
                @Override public Logger apply(Boolean init) {
                    if (init)
                        ctx.reconfigure();

                    return (Logger)LogManager.getRootLogger();
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Gets {@link IgniteLogger} wrapper around log4j logger for the given
     * category. If category is {@code null}, then root logger is returned. If
     * category is an instance of {@link Class} then {@code (Class)ctgr).getName()}
     * is used as category name.
     *
     * @param ctgr {@inheritDoc}
     * @return {@link IgniteLogger} wrapper around log4j logger.
     */
    @Override public Log4J2Logger getLogger(Object ctgr) {
        if (ctgr == null)
            return new Log4J2Logger((Logger)LogManager.getRootLogger(), cfg);

        if (ctgr instanceof Class) {
            String name = ((Class<?>)ctgr).getName();

            return new Log4J2Logger((Logger)LogManager.getLogger(name), cfg);
        }

        String name = ctgr.toString();

        return new Log4J2Logger((Logger)LogManager.getLogger(name), cfg);
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        trace(null, msg);
    }

    /** {@inheritDoc} */
    @Override public void trace(@Nullable String marker, String msg) {
        if (!isTraceEnabled())
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

        impl.trace(getMarkerOrNull(marker), msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        debug(null, msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(@Nullable String marker, String msg) {
        if (!isDebugEnabled())
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        impl.debug(getMarkerOrNull(marker), msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        info(null, msg);
    }

    /** {@inheritDoc} */
    @Override public void info(@Nullable String marker, String msg) {
        if (!isInfoEnabled())
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

        impl.info(getMarkerOrNull(marker), msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        warning(null, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void warning(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.warn(getMarkerOrNull(marker), msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        error(null, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.error(getMarkerOrNull(marker), msg, e);
    }

    /** Returns Marker object for the specified name, or null if the name is null */
    private Marker getMarkerOrNull(@Nullable String marker) {
        return marker != null ? MarkerManager.getMarker(marker) : null;
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
    @Override public boolean isQuiet() {
        return quiet;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Log4J2Logger.class, this, "config", cfg);
    }
}

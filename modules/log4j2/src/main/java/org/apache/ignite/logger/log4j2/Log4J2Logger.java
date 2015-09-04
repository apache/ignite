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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.AppenderControl;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
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

    /** Auto added at verbose mode console logger (nullable). */
    @GridToStringExclude
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private Logger consoleLog;

    /** Quiet flag. */
    private final boolean quiet;

    /** Node ID. */
    private volatile UUID nodeId;

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Log4j implementation to use.
     * @param consoleLog Cosole logger (optional).
     */
    private Log4J2Logger(final Logger impl, @Nullable final Logger consoleLog) {
        assert impl != null;
        this.impl = impl;
        this.consoleLog = consoleLog;

        quiet = quiet0;
    }

    /**
     * Creates new logger with given configuration {@code path}.
     *
     * @param path Path to log4j configuration XML file.
     * @throws IgniteCheckedException Thrown in case logger can't be created.
     */
    public Log4J2Logger(String path) throws IgniteCheckedException {
        if (path == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j must be specified.");

        final URL cfgUrl = U.resolveIgniteUrl(path);

        if (cfgUrl == null)
            throw new IgniteCheckedException("Log4j configuration path was not found: " + path);

        addConsoleAppenderIfNeeded(new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LogManager.ROOT_LOGGER_NAME, cfgUrl.toString());

                return (Logger)LogManager.getRootLogger();
            }
        });

        quiet = quiet0;
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
    }

    /**
     * Sets level for internal log4j implementation.
     *
     * @param level Log level to set.
     */
    public void setLevel(Level level) {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(false);

        Configuration conf = ctx.getConfiguration();

        conf.getLoggerConfig(impl.getName()).setLevel(level);

        ctx.updateLoggers(conf);
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
                                return normilize(((FileAppender)innerApp).getFileName());

                            if (innerApp instanceof RollingFileAppender)
                                return normilize(((RollingFileAppender)innerApp).getFileName());
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
    private String normilize(String path) {
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
                consoleLog = createConsoleLogger();
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
    public static Logger createConsoleLogger() {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(true);

        Configuration cfg = ctx.getConfiguration();

        PatternLayout layout = PatternLayout.createLayout("[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n", null, null,
            Charset.defaultCharset(), false, false, null, null);

        final Appender consoleApp = ConsoleAppender.createAppender(layout, null, null, CONSOLE_APPENDER, null, null);
        consoleApp.start();

        AppenderRef ref = AppenderRef.createAppenderRef(CONSOLE_APPENDER, Level.TRACE, null);

        AppenderRef[] refs = {ref};

        LoggerConfig logCfg = LoggerConfig.createLogger("false", Level.INFO, LogManager.ROOT_LOGGER_NAME, "",
            refs, null, null, null);

        logCfg.addAppender(consoleApp, null, null);
        cfg.addAppender(consoleApp);

        cfg.addLogger(LogManager.ROOT_LOGGER_NAME, logCfg);

        ctx.updateLoggers(cfg);

        return (Logger)LogManager.getContext().getLogger(LogManager.ROOT_LOGGER_NAME);
    }

    /** {@inheritDoc} */
    @Override public void setNodeId(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        this.nodeId = nodeId;

        // Set nodeId as system variable to be used at configuration.
        System.setProperty(NODE_ID, U.id8(nodeId));

        ((LoggerContext)LogManager.getContext(false)).reconfigure();
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
            return new Log4J2Logger((Logger)LogManager.getRootLogger(),
                consoleLog == null ? null : (Logger)LogManager.getContext().getLogger(""));

        if (ctgr instanceof Class) {
            String name = ((Class<?>)ctgr).getName();

            return new Log4J2Logger((Logger)LogManager.getLogger(name),
                consoleLog == null ? null : (Logger)LogManager.getContext().getLogger(name));
        }

        String name = ctgr.toString();

        return new Log4J2Logger((Logger)LogManager.getLogger(name),
            consoleLog == null ? null : (Logger)LogManager.getContext().getLogger(name));
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!isTraceEnabled())
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

        impl.trace(msg);

        if (consoleLog != null)
            consoleLog.trace(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!isDebugEnabled())
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        impl.debug(msg);

        if (consoleLog != null)
            consoleLog.debug(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (!isInfoEnabled())
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

        impl.info(msg);

        if (consoleLog != null)
            consoleLog.info(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warn(msg);

        if (consoleLog != null)
            consoleLog.warn(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        impl.warn(msg, e);

        if (consoleLog != null)
            consoleLog.warn(msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.error(msg);

        if (consoleLog != null)
            consoleLog.error(msg);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.error(msg, e);

        if (consoleLog != null)
            consoleLog.error(msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isTraceEnabled() || (consoleLog != null && consoleLog.isTraceEnabled());
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isDebugEnabled() || (consoleLog != null && consoleLog.isDebugEnabled());
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isInfoEnabled() || (consoleLog != null && consoleLog.isInfoEnabled());
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return quiet;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Log4J2Logger.class, this);
    }
}

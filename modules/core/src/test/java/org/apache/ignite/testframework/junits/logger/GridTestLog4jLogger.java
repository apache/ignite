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

package org.apache.ignite.testframework.junits.logger;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSOLE_APPENDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.OFF;
import static org.apache.logging.log4j.Level.TRACE;
import static org.apache.logging.log4j.core.Filter.Result.ACCEPT;
import static org.apache.logging.log4j.core.Filter.Result.DENY;
import static org.apache.logging.log4j.core.appender.ConsoleAppender.Target.SYSTEM_ERR;
import static org.apache.logging.log4j.core.appender.ConsoleAppender.Target.SYSTEM_OUT;

/**
 * Log4j-based implementation for logging. This logger should be used
 * by loaders that have prefer <a target=_new href="https://logging.apache.org/log4j/2.x/">log4j2</a>-based logging.
 * <p>
 * Here is a typical example of configuring log4j logger in Ignite configuration file:
 * <pre name="code" class="xml">
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.log4j.Log4J2Logger"&gt;
 *              &lt;constructor-arg type="java.lang.String" value="config/ignite-log4j.xml"/&gt;
 *          &lt;/bean>
 *      &lt;/property&gt;
 * </pre>
 * and from your code:
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      URL xml = U.resolveIgniteUrl("config/custom-log42j.xml");
 *      IgniteLogger log = new Log4J2Logger(xml);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 *
 * Please take a look at <a target=_new href="https://logging.apache.org/log4j/2.x/>Apache Log4j 2.x</a>
 * for additional information.
 * <p>
 * It's recommended to use Ignite logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.LoggerResource} annotation about logger
 * injection.
 */
public class GridTestLog4jLogger implements IgniteLoggerEx {
    /** */
    public static final String FILE = "FILE";

    /** */
    public static final String CONSOLE = "CONSOLE";

    /** */
    public static final String CONSOLE_ERROR = "CONSOLE_ERR";

    /** */
    private static final String NODE_ID = "nodeId";

    /** */
    private static final String APP_ID = "appId";

    /** */
    public static final PatternLayout DEFAULT_PATTERN_LAYOUT = PatternLayout.newBuilder()
        .withPattern("[%d{ISO8601}][%-5p][%t][%c{1}] %m%n")
        .build();

    /** Appenders. */
    private static Collection<FileAppender> fileAppenders = new GridConcurrentHashSet<>();

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

    /**
     * Creates new logger and automatically detects if root logger already
     * has appenders configured. If it does not, the root logger will be
     * configured with default appender (analogous to calling
     * {@link #GridTestLog4jLogger(boolean) Log4j2Logger(boolean)}
     * with parameter {@code true}, otherwise, existing appenders will be used.
     */
    public GridTestLog4jLogger() {
        this(!isConfigured());
    }

    /**
     * Creates new logger. If initialize parameter is {@code true} the Log4j
     * logger will be initialized with default console appender and {@code INFO}
     * log level.
     *
     * @param init If {@code true}, then a default console appender with
     *      following pattern layout will be created: {@code %d{ISO8601} %-5p [%c{1}] %m%n}.
     *      If {@code false}, then no implicit initialization will take place,
     *      and {@code Log4j2} should be configured prior to calling this
     *      constructor.
     */
    public GridTestLog4jLogger(boolean init) {
        impl = LogManager.getRootLogger();

        if (init) {
            // Implementation has already been inited, passing NULL.
            addConsoleAppenderIfNeeded(INFO, null);

            quiet = quiet0;
        }
        else
            quiet = true;

        cfg = null;
    }

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Log4j implementation to use.
     */
    public GridTestLog4jLogger(final Logger impl) {
        assert impl != null;

        addConsoleAppenderIfNeeded(null, new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                return impl;
            }
        });

        quiet = quiet0;
        cfg = null;
    }

    /**
     * Creates new logger with given configuration {@code path}.
     *
     * @param path Path to log4j configuration XML file.
     * @throws IgniteCheckedException Thrown in case logger can't be created.
     */
    public GridTestLog4jLogger(String path) throws IgniteCheckedException {
        if (path == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j2 must be specified.");

        this.cfg = path;

        final URL cfgUrl = U.resolveIgniteUrl(path);

        if (cfgUrl == null)
            throw new IgniteCheckedException("Log4j2 configuration path was not found: " + path);

        addConsoleAppenderIfNeeded(null, new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LoggerConfig.ROOT, cfgUrl.getPath());

                return LogManager.getRootLogger();
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
    public GridTestLog4jLogger(File cfgFile) throws IgniteCheckedException {
        if (cfgFile == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j must be specified.");

        if (!cfgFile.exists() || cfgFile.isDirectory())
            throw new IgniteCheckedException("Log4j2 configuration path was not found or is a directory: " + cfgFile);

        cfg = cfgFile.getAbsolutePath();

        addConsoleAppenderIfNeeded(null, new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LoggerConfig.ROOT, cfg);

                return LogManager.getRootLogger();
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
    public GridTestLog4jLogger(final URL cfgUrl) throws IgniteCheckedException {
        if (cfgUrl == null)
            throw new IgniteCheckedException("Configuration XML file for Log4j must be specified.");

        cfg = cfgUrl.getPath();

        addConsoleAppenderIfNeeded(null, new C1<Boolean, Logger>() {
            @Override public Logger apply(Boolean init) {
                if (init)
                    Configurator.initialize(LoggerConfig.ROOT, cfg);

                return LogManager.getRootLogger();
            }
        });

        quiet = quiet0;
    }

    /**
     * Checks if Log4j is already configured within this VM or not.
     *
     * @return {@code True} if log4j was already configured, {@code false} otherwise.
     */
    public static boolean isConfigured() {
        return !(LoggerContext.getContext(false).getConfiguration() instanceof DefaultConfiguration);
    }

    /**
     * Sets level for internal log4j implementation.
     *
     * @param level Log level to set.
     */
    public void setLevel(Level level) {
        Configurator.setLevel(impl.getName(), level);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        FileAppender fapp = F.first(fileAppenders);

        return fapp != null ? fapp.getFileName() : null;
    }

    /** {@inheritDoc} */
    @Override public void addConsoleAppender() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        // No-op.
    }

    /**
     * Adds console appender when needed with some default logging settings.
     *
     * @param logLevel Optional log level.
     * @param implInitC Optional log implementation init closure.
     */
    private void addConsoleAppenderIfNeeded(@Nullable Level logLevel,
        @Nullable IgniteClosure<Boolean, Logger> implInitC) {
        if (inited) {
            if (implInitC != null)
                // Do not init.
                impl = implInitC.apply(false);

            return;
        }

        synchronized (mux) {
            if (inited) {
                if (implInitC != null)
                    // Do not init.
                    impl = implInitC.apply(false);

                return;
            }

            if (implInitC != null)
                // Init logger impl.
                impl = implInitC.apply(true);

            boolean quiet = Boolean.valueOf(System.getProperty(IGNITE_QUIET, "true"));

            T2<Boolean, Boolean> consoleAppendersFound = isConsoleAppendersConfigured();

            if (consoleAppendersFound.get1() && quiet)
                // User configured console appender, but log is quiet.
                quiet = false;

            if (!consoleAppendersFound.get1() && !quiet && Boolean.valueOf(System.getProperty(IGNITE_CONSOLE_APPENDER, "true"))) {
                configureConsoleAppender(consoleAppendersFound.get2() ? INFO : OFF);

                if (logLevel != null)
                    Configurator.setLevel(impl.getName(), logLevel);
            }

            quiet0 = quiet;
            inited = true;
        }
    }

    /** @return Pair of flags that determines whether SYSTEM_OUT and SYSTEM_ERR appenders are configured respectively. */
    private T2<Boolean, Boolean> isConsoleAppendersConfigured() {
        Configuration cfg = LoggerContext.getContext(false).getConfiguration();

        if (cfg instanceof DefaultConfiguration)
            return new T2<>(false, false);

        boolean sysOut = false;
        boolean sysErr = false;

        for (
            LoggerConfig logCfg = cfg.getLoggerConfig(impl.getName());
            logCfg != null && (!sysOut || !sysErr);
            logCfg = logCfg.getParent()
        ) {
            for (Appender appender : logCfg.getAppenders().values()) {
                if (appender instanceof ConsoleAppender) {
                    if (((ConsoleAppender)appender).getTarget() == SYSTEM_ERR)
                        sysErr = true;

                    if (((ConsoleAppender)appender).getTarget() == SYSTEM_OUT)
                        sysOut = true;
                }
            }
        }

        return new T2<>(sysOut, sysErr);
    }

    /**
     * Creates console appender with some reasonable default logging settings.
     *
     * @param minLvl Minimal logging level.
     * @return Logger with auto configured console appender.
     */
    public Logger configureConsoleAppender(Level minLvl) {
        // from http://logging.apache.org/log4j/2.x/manual/customconfig.html
        LoggerContext ctx = LoggerContext.getContext(false);

        Configuration cfg = ctx.getConfiguration();

        if (cfg instanceof DefaultConfiguration) {
            ConfigurationBuilder<BuiltConfiguration> cfgBuilder = ConfigurationBuilderFactory.newConfigurationBuilder();

            RootLoggerComponentBuilder rootLog = cfgBuilder.newRootLogger(INFO);

            cfg = cfgBuilder.add(rootLog).build();

            addConsoleAppender(cfg, minLvl);

            ctx.reconfigure(cfg);
        }
        else {
            addConsoleAppender(cfg, minLvl);

            ctx.updateLoggers();
        }

        return ctx.getRootLogger();
    }

    /** */
    private void addConsoleAppender(Configuration logCfg, Level minLvl) {
        Appender consoleApp = ConsoleAppender.newBuilder()
            .setName(CONSOLE)
            .setTarget(SYSTEM_OUT)
            .setLayout(DEFAULT_PATTERN_LAYOUT)
            .setFilter(LevelRangeFilter.createFilter(minLvl, TRACE, ACCEPT, DENY))
            .build();

        consoleApp.start();

        logCfg.addAppender(consoleApp);
        logCfg.getRootLogger().addAppender(consoleApp, Level.TRACE, null);
    }

    /**
     * Adds file appender.
     *
     * @param a Appender.
     */
    public static void addAppender(FileAppender a) {
        A.notNull(a, "a");

        fileAppenders.add(a);
    }

    /**
     * Removes file appender.
     *
     * @param a Appender.
     */
    public static void removeAppender(FileAppender a) {
        A.notNull(a, "a");

        fileAppenders.remove(a);
    }

    /** {@inheritDoc} */
    @Override public void setApplicationAndNode(@Nullable String application, @Nullable UUID nodeId) {
        if (nodeId != null)
            System.setProperty(NODE_ID, U.id8(nodeId));

        System.setProperty(APP_ID, application != null ? application : "ignite");
    }

    /**
     * Gets files for all registered file appenders.
     *
     * @return List of files.
     */
    public static Collection<String> logFiles() {
        Collection<String> res = new ArrayList<>(fileAppenders.size());

        for (FileAppender a : fileAppenders)
            res.add(a.getFileName());

        return res;
    }

    /**
     * Gets {@link org.apache.ignite.IgniteLogger} wrapper around log4j logger for the given
     * category. If category is {@code null}, then root logger is returned. If
     * category is an instance of {@link Class} then {@code (Class)ctgr).getName()}
     * is used as category name.
     *
     * @param ctgr {@inheritDoc}
     * @return {@link org.apache.ignite.IgniteLogger} wrapper around log4j logger.
     */
    @Override public GridTestLog4jLogger getLogger(Object ctgr) {
        return new GridTestLog4jLogger(ctgr == null
            ? LogManager.getRootLogger()
            : ctgr instanceof Class
                ? LogManager.getLogger(((Class<?>)ctgr).getName())
                : LogManager.getLogger(ctgr.toString()));
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!impl.isTraceEnabled())
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

            assert impl.isTraceEnabled() : "Logging at TRACE level without checking if TRACE level is enabled: " + msg;

        impl.trace(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!impl.isDebugEnabled())
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        assert impl.isDebugEnabled() : "Logging at DEBUG level without checking if DEBUG level is enabled: " + msg;

        impl.debug(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (!impl.isInfoEnabled())
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

        assert impl.isInfoEnabled() : "Logging at INFO level without checking if INFO level is enabled: " + msg;

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
        return S.toString(GridTestLog4jLogger.class, this, "config", cfg);
    }

    /** */
    public static void removeAllRootLoggerAppenders() {
        LoggerConfig rootLogCfg = LoggerContext.getContext(false).getConfiguration().getRootLogger();

        for (Appender app : rootLogCfg.getAppenders().values()) {
            rootLogCfg.removeAppender(app.getName());

            app.stop();
        }

        LoggerContext.getContext(false).updateLoggers();
    }

    /** */
    public static void addRootLoggerAppender(Level lvl, Appender app) {
        LoggerContext ctx = LoggerContext.getContext(false);

        app.start();

        ctx.getConfiguration().addAppender(app);
        ctx.getConfiguration().getRootLogger().addAppender(app, lvl, null);

        ctx.updateLoggers();
    }

    /** */
    public static void removeRootLoggerAppender(String name) {
        LoggerConfig rootLogCfg = LoggerContext.getContext(false).getConfiguration().getRootLogger();

        Appender app = rootLogCfg.getAppenders().get(name);

        if (app == null)
            return;

        app.stop();

        rootLogCfg.removeAppender(name);

        LoggerContext.getContext(false).updateLoggers();

    }
}

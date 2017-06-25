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
import java.io.InputStream;
import java.net.URL;
import java.util.UUID;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.jetbrains.annotations.Nullable;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSOLE_APPENDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;

/**
 * Logger to use with Java logging. Implementation simply delegates to Java Logging.
 * <p>
 * Here is an example of configuring Java logger in Ignite configuration Spring
 * file to work over log4j implementation. Note that we use the same configuration file
 * as we provide by default:
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.java.JavaLogger"&gt;
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
 *          &lt;bean class="org.apache.ignite.logger.java.JavaLogger"/&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * And the same configuration if you'd like to configure Ignite in your code:
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      IgniteLogger log = new JavaLogger(Logger.global);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or which is actually the same:
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      IgniteLogger log = new JavaLogger();
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * Please take a look at <a target=_new href="http://docs.oracle.com/javase/7/docs/api/java/util/logging/Logger.html">Logger javadoc</a>
 * for additional information.
 * <p>
 * It's recommended to use Ignite logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.LoggerResource} annotation about logger
 * injection.
 */
public class JavaLogger implements IgniteLogger, LoggerNodeIdAware {
    /** */
    public static final String DFLT_CONFIG_PATH = "config/java.util.logging.properties";

    /** */
    private static final Object mux = new Object();

    /** */
    private static volatile boolean inited;

    /** */
    private static volatile boolean quiet0;

    /** Java Logging implementation proxy. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private Logger impl;

    /** Quiet flag. */
    private final boolean quiet;

    /** Work directory. */
    private volatile String workDir;

    /** Node ID. */
    private volatile UUID nodeId;

    /**
     * Creates new logger.
     */
    public JavaLogger() {
        this(!isConfigured());
    }

    /**
     * Checks if logger is already configured within this VM or not.
     *
     * @return {@code True} if logger was already configured, {@code false} otherwise.
     */
    public static boolean isConfigured() {
        return System.getProperty("java.util.logging.config.file") != null;
    }

    /**
     * Reads default JUL configuration.
     */
    private void defaultConfiguration() {
        final URL cfgUrl = U.resolveIgniteUrl(DFLT_CONFIG_PATH);

        if (cfgUrl == null) {
            error("Failed to resolve default logging config file: " + DFLT_CONFIG_PATH);

            return;
        }

        try (InputStream in = cfgUrl.openStream()) {
            LogManager.getLogManager().readConfiguration(in);
        }
        catch (IOException e) {
            error("Failed to read logging configuration: " + cfgUrl, e);
        }
    }

    /**
     * Creates new logger.
     *
     * @param init If {@code true}, then a default console appender will be created.
     *      If {@code false}, then no implicit initialization will take place,
     *      and java logger should be configured prior to calling this constructor.
     */
    public JavaLogger(boolean init) {
        impl = Logger.getLogger("");

        if (init) {
            // Implementation has already been inited, passing NULL.
            configure(null);

            quiet = quiet0;
        }
        else
            quiet = true;
    }

    /**
     * Creates new logger with given implementation.
     *
     * @param impl Java Logging implementation to use.
     */
    public JavaLogger(final Logger impl) {
        assert impl != null;

        configure(impl);

        quiet = quiet0;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new JavaLogger(ctgr == null ? Logger.getLogger("") : Logger.getLogger(
            ctgr instanceof Class ? ((Class)ctgr).getName() : String.valueOf(ctgr)));
    }

    /**
     * Configures handlers when needed.
     *
     * @param initImpl Optional log implementation.
     */
    private void configure(@Nullable Logger initImpl) {
        if (initImpl != null)
            impl = initImpl;

        if (inited)
            return;

        synchronized (mux) {
            if (inited)
                return;

            if (isConfigured()) {
                boolean consoleHndFound = findHandler(impl, ConsoleHandler.class) != null;

                // User configured console appender, thus log is not quiet.
                quiet0 = !consoleHndFound;
                inited = true;

                return;
            }

            defaultConfiguration();

            boolean quiet = Boolean.valueOf(System.getProperty(IGNITE_QUIET, "true"));
            boolean useConsoleAppender = Boolean.valueOf(System.getProperty(IGNITE_CONSOLE_APPENDER, "true"));

            if (useConsoleAppender) {
                ConsoleHandler consoleHnd = findHandler(impl, ConsoleHandler.class);

                if (consoleHnd != null)
                    consoleHnd.setLevel(quiet ? SEVERE : INFO);
                else
                    System.err.println("Console logging handler is not configured.");
            }
            else {
                Handler[] handlers = Logger.getLogger("").getHandlers();

                if  (!F.isEmpty(handlers)) {
                    for (Handler h : handlers) {
                        if (h instanceof ConsoleHandler)
                            impl.removeHandler(h);
                    }
                }
            }

            quiet0 = quiet;
            inited = true;
        }
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (!impl.isLoggable(FINEST))
            warning("Logging at TRACE level without checking if TRACE level is enabled: " + msg);

        impl.finest(msg);
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (!impl.isLoggable(FINE))
            warning("Logging at DEBUG level without checking if DEBUG level is enabled: " + msg);

        impl.fine(msg);
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (!impl.isLoggable(INFO))
            warning("Logging at INFO level without checking if INFO level is enabled: " + msg);

        impl.info(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warning(msg);
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        impl.log(WARNING, msg, e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.severe(msg);
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return quiet;
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        impl.log(SEVERE, msg, e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isLoggable(FINEST);
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isLoggable(FINE);
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isLoggable(INFO);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        JavaLoggerFileHandler gridFileHnd = findHandler(impl, JavaLoggerFileHandler.class);

        if (gridFileHnd != null)
            return gridFileHnd.fileName();

        FileHandler fileHnd = findHandler(impl, FileHandler.class);

        return fileName(fileHnd);
    }

    /**
     * @param fileHnd File handler.
     * @return Current log file or {@code null} if it can not be retrieved from file handler.
     */
    @Nullable static String fileName(FileHandler fileHnd) {
        if (fileHnd == null)
            return null;

        try {
            File[] logFiles = U.field(fileHnd, "files");

            return logFiles[0].getAbsolutePath();
        }
        catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Set work directory.
     *
     * @param workDir Work directory.
     */
    public void setWorkDirectory(String workDir) {
        this.workDir = workDir;
    }

    /** {@inheritDoc} */
    @Override public void setNodeId(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        if (this.nodeId != null)
            return;

        synchronized (mux) {
            // Double check.
            if (this.nodeId != null)
                return;

            this.nodeId = nodeId;
        }

        JavaLoggerFileHandler fileHnd = findHandler(impl, JavaLoggerFileHandler.class);

        if (fileHnd == null)
            return;

        try {
            fileHnd.nodeId(nodeId, workDir);
        }
        catch (IgniteCheckedException | IOException e) {
            throw new RuntimeException("Failed to enable file handler.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Returns first found handler of specified class type or {@code null} if that handler isn't configured.
     *
     * @param log Logger.
     * @param cls Class.
     * @param <T> Class type.
     * @return First found handler of specified class type or {@code null} if that handler isn't configured.
     */
    @SuppressWarnings("unchecked")
    private static <T> T findHandler(Logger log, Class<T> cls) {
        while (log != null) {
            for (Handler hnd : log.getHandlers()) {
                if (cls.isInstance(hnd))
                    return (T)hnd;
            }

            log = log.getParent();
        }

        return null;
    }
}

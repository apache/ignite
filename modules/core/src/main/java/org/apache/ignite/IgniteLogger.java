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

package org.apache.ignite;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * This interface defines basic logging functionality used throughout the system. We had to
 * abstract it out so that we can use whatever logging is used by the hosting environment.
 * Currently, <a target=_new href="http://https://logging.apache.org/log4j/2.x/">log4j2</a>,
 * <a target=_new href="https://docs.jboss.org/hibernate/orm/5.4/topical/html_single/logging/Logging.html">JBoss</a>,
 * <a target=_new href="http://jakarta.apache.org/commons/logging/">JCL</a> and
 * console logging are provided as supported implementations.
 * <p>
 * Ignite logger could be configured either from code (for example log4j2 logger):
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      URL xml = U.resolveIgniteUrl("config/custom-log4j.xml");
 *      IgniteLogger log = new Log4J2Logger(xml);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or in grid configuration file (see JCL logger example below):
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.jcl.JclLogger"&gt;
 *              &lt;constructor-arg type="org.apache.commons.logging.Log"&gt;
 *                  &lt;bean class="org.apache.commons.logging.impl.Log4J2Logger"&gt;
 *                      &lt;constructor-arg type="java.lang.String" value="config/ignite-log4j.xml"/&gt;
 *                  &lt;/bean&gt;
 *              &lt;/constructor-arg&gt;
 *          &lt;/bean&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * It's recommended to use Ignite's logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.LoggerResource} annotation about logger
 * injection.
 * <h1 class="header">Quiet Mode</h1>
 * By default Ignite starts in "quiet" mode suppressing {@code INFO} and {@code DEBUG}
 * log output. If system property {@code IGNITE_QUIET} is set to {@code false} than Ignition
 * will operate in normal un-suppressed logging mode. Note that all output in "quiet" mode is
 * done through standard output (STDOUT).
 * <p>
 * Note that Ignite's standard startup scripts <tt>$IGNITE_HOME/bin/ignite.{sh|bat}</tt> start
 * by default in "quiet" mode. Both scripts accept {@code -v} arguments to turn off "quiet" mode.
 */
@GridToStringExclude
public interface IgniteLogger {
    /**
     * Marker for log messages that are useful in development environments, but not in production.
     */
    String DEV_ONLY = "DEV_ONLY";

    /**
     * Creates new logger with given category based off the current instance.
     *
     * @param ctgr Category for new logger.
     * @return New logger with given category.
     */
    IgniteLogger getLogger(Object ctgr);

    /**
     * Logs out trace message.
     *
     * @param msg Trace message.
     */
    void trace(String msg);

    /**
     * Logs out trace message.
     * The default implementation calls {@code this.trace(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Trace message.
     */
    default void trace(@Nullable String marker, String msg) {
        trace(msg);
    }

    /**
     * Logs out debug message.
     *
     * @param msg Debug message.
     */
    void debug(String msg);

    /**
     * Logs out debug message.
     * The default implementation calls {@code this.debug(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Debug message.
     */
    default void debug(@Nullable String marker, String msg) {
        debug(msg);
    }

    /**
     * Logs out information message.
     *
     * @param msg Information message.
     */
    void info(String msg);

    /**
     * Logs out information message.
     * The default implementation calls {@code this.info(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Information message.
     */
    default void info(@Nullable String marker, String msg) {
        info(msg);
    }

    /**
     * Logs out warning message.
     *
     * @param msg Warning message.
     */
    default void warning(String msg) {
        warning(msg, null);
    }

    /**
     * Logs out warning message with optional exception.
     *
     * @param msg Warning message.
     * @param e Optional exception (can be {@code null}).
     */
    void warning(String msg, @Nullable Throwable e);

    /**
     * Logs out warning message with optional exception.
     * The default implementation calls {@code this.warning(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Warning message.
     * @param e Optional exception (can be {@code null}).
     */
    default void warning(@Nullable String marker, String msg, @Nullable Throwable e) {
        warning(msg, e);
    }

    /**
     * Logs out error message.
     *
     * @param msg Error message.
     */
    default void error(String msg) {
        error(null, msg, null);
    }

    /**
     * Logs error message with optional exception.
     *
     * @param msg Error message.
     * @param e Optional exception (can be {@code null}).
     */
    void error(String msg, @Nullable Throwable e);

    /**
     * Logs error message with optional exception.
     * The default implementation calls {@code this.error(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Error message.
     * @param e Optional exception (can be {@code null}).
     */
    default void error(@Nullable String marker, String msg, @Nullable Throwable e) {
        error(msg, e);
    }

    /**
     * Tests whether {@code trace} level is enabled.
     *
     * @return {@code true} in case when {@code trace} level is enabled, {@code false} otherwise.
     */
    boolean isTraceEnabled();

    /**
     * Tests whether {@code debug} level is enabled.
     *
     * @return {@code true} in case when {@code debug} level is enabled, {@code false} otherwise.
     */
    boolean isDebugEnabled();

    /**
     * Tests whether {@code info} level is enabled.
     *
     * @return {@code true} in case when {@code info} level is enabled, {@code false} otherwise.
     */
    boolean isInfoEnabled();

    /**
     * Tests whether Logger is in "Quiet mode".
     *
     * @return {@code true} "Quiet mode" is enabled, {@code false} otherwise
     */
    boolean isQuiet();

    /**
     * Gets name of the file being logged to if one is configured or {@code null} otherwise.
     *
     * @return Name of the file being logged to if one is configured or {@code null} otherwise.
     */
    String fileName();
}

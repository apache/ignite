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
 * Currently, <a target=_new href="http://logging.apache.org/log4j/1.2/">log4j</a>,
 * <a target=_new href="http://docs.jboss.org/hibernate/orm/4.3/topical/html/logging/Logging">JBoss</a>,
 * <a target=_new href="http://jakarta.apache.org/commons/logging/">JCL</a> and
 * console logging are provided as supported implementations.
 * <p>
 * Ignite logger could be configured either from code (for example log4j logger):
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      URL xml = U.resolveIgniteUrl("config/custom-log4j.xml");
 *      IgniteLogger log = new Log4JLogger(xml);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or in grid configuration file (see JCL logger example below):
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.jcl.JclLogger"&gt;
 *              &lt;constructor-arg type="org.apache.commons.logging.Log"&gt;
 *                  &lt;bean class="org.apache.commons.logging.impl.Log4JLogger"&gt;
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
     * Creates new logger with given category based off the current instance.
     *
     * @param ctgr Category for new logger.
     * @return New logger with given category.
     */
    public IgniteLogger getLogger(Object ctgr);

    /**
     * Logs out trace message.
     *
     * @param msg Trace message.
     */
    public void trace(String msg);

    /**
     * Logs out debug message.
     *
     * @param msg Debug message.
     */
    public void debug(String msg);

    /**
     * Logs out information message.
     *
     * @param msg Information message.
     */
    public void info(String msg);

    /**
     * Logs out warning message.
     *
     * @param msg Warning message.
     */
    public void warning(String msg);

    /**
     * Logs out warning message with optional exception.
     *
     * @param msg Warning message.
     * @param e Optional exception (can be {@code null}).
     */
    public void warning(String msg, @Nullable Throwable e);

    /**
     * Logs out error message.
     *
     * @param msg Error message.
     */
    public void error(String msg);

    /**
     * Logs error message with optional exception.
     *
     * @param msg Error message.
     * @param e Optional exception (can be {@code null}).
     */
    public void error(String msg, @Nullable Throwable e);

    /**
     * Tests whether {@code trace} level is enabled.
     *
     * @return {@code true} in case when {@code trace} level is enabled, {@code false} otherwise.
     */
    public boolean isTraceEnabled();

    /**
     * Tests whether {@code debug} level is enabled.
     *
     * @return {@code true} in case when {@code debug} level is enabled, {@code false} otherwise.
     */
    public boolean isDebugEnabled();

    /**
     * Tests whether {@code info} level is enabled.
     *
     * @return {@code true} in case when {@code info} level is enabled, {@code false} otherwise.
     */
    public boolean isInfoEnabled();

    /**
     * Tests whether Logger is in "Quiet mode".
     *
     * @return {@code true} "Quiet mode" is enabled, {@code false} otherwise
     */
    public boolean isQuiet();

    /**
     * Gets name of the file being logged to if one is configured or {@code null} otherwise.
     *
     * @return Name of the file being logged to if one is configured or {@code null} otherwise.
     */
    public String fileName();
}

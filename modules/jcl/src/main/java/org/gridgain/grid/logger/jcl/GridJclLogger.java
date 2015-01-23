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

package org.gridgain.grid.logger.jcl;

import org.apache.commons.logging.*;
import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * This logger wraps any JCL (<a target=_blank href="http://jakarta.apache.org/commons/logging/">Jakarta Commons Logging</a>)
 * loggers. Implementation simply delegates to underlying JCL logger. This logger
 * should be used by loaders that have JCL-based internal logging (e.g., Websphere).
 * <p>
 * Here is an example of configuring JCL logger in GridGain configuration Spring
 * file to work over log4j implementation. Note that we use the same configuration file
 * as we provide by default:
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.gridgain.grid.logger.jcl.GridJclLogger"&gt;
 *              &lt;constructor-arg type="org.apache.commons.logging.Log"&gt;
 *                  &lt;bean class="org.apache.commons.logging.impl.Log4JLogger"&gt;
 *                      &lt;constructor-arg type="java.lang.String" value="config/gridgain-log4j.xml"/&gt;
 *                  &lt;/bean&gt;
 *              &lt;/constructor-arg&gt;
 *          &lt;/bean&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * If you are using system properties to configure JCL logger use following configuration:
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.gridgain.grid.logger.jcl.GridJclLogger"/&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * And the same configuration if you'd like to configure GridGain in your code:
 * <pre name="code" class="java">
 *      GridConfiguration cfg = new GridConfiguration();
 *      ...
 *      GridLogger log = new GridJclLogger(new Log4JLogger("config/gridgain-log4j.xml"));
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or following for the configuration by means of system properties:
 * <pre name="code" class="java">
 *      GridConfiguration cfg = new GridConfiguration();
 *      ...
 *      GridLogger log = new GridJclLogger();
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 *
 * <p>
 * It's recommended to use GridGain logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.IgniteLoggerResource} annotation about logger
 * injection.
 */
public class GridJclLogger implements IgniteLogger {
    /** */
    private static final long serialVersionUID = 0L;

    /** JCL implementation proxy. */
    private Log impl;

    /**
     * Creates new logger.
     */
    public GridJclLogger() {
        this(LogFactory.getLog(GridJclLogger.class.getName()));
    }

    /**
     * Creates new logger with given implementation.
     *
     * @param impl JCL implementation to use.
     */
    public GridJclLogger(Log impl) {
        assert impl != null;

        this.impl = impl;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new GridJclLogger(LogFactory.getLog(
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
        return "GridJclLogger [impl=" + impl + ']';
    }
}

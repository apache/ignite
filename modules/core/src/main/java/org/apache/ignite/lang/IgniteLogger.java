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

package org.apache.ignite.lang;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Ignite logger wraps system logger for more convenient access.
 */
public class IgniteLogger {
    /**
     * Creates logger for class.
     *
     * @param cls The class for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls) {
        return new IgniteLogger(cls);
    }

    /** Logger delegate. */
    private final System.Logger log;

    /**
     * @param cls The class for a logger.
     */
    protected IgniteLogger(@NotNull Class<?> cls) {
        log = System.getLogger(Objects.requireNonNull(cls).getName());
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void info(String msg, Object... params) {
        log.log(INFO, msg, params);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void debug(String msg, Object... params) {
        log.log(DEBUG, msg, params);
    }

    /**
     * @param msg The message.
     * @param th A {@code Throwable} associated with the log message;
     */
    public void debug(String msg, Throwable th) {
        log.log(DEBUG, msg, th);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void warn(String msg, Object... params) {
        log.log(WARNING, msg, params);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void error(String msg, Object... params) {
        log.log(ERROR, msg, params);
    }

    /**
     * @param msg The message.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void error(String msg, Throwable th) {
        log.log(ERROR, msg, th);
    }

    /**
     * @param msg The message.
     * @param params Parameters.
     */
    public void trace(String msg, Object... params) {
        log.log(TRACE, msg, params);
    }

    /**
     * @return {@code true} if the TRACE log message level is currently being logged.
     */
    public boolean isTraceEnabled() {
        return log.isLoggable(TRACE);
    }

    /**
     * @return {@code true} if the DEBUG log message level is currently being logged.
     */
    public boolean isDebugEnabled() {
        return log.isLoggable(DEBUG);
    }

    /**
     * @return {@code true} if the INFO log message level is currently being logged.
     */
    public boolean isInfoEnabled() {
        return log.isLoggable(INFO);
    }
}

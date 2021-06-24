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

import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

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
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void info(String msg, Object... params) {
        logInternal(Level.INFO, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#INFO} level with associated exception
     * {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void info(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(Level.INFO, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void info(String msg, Throwable th) {
        log.log(Level.INFO, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void debug(String msg, Object... params) {
        logInternal(Level.DEBUG, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#DEBUG} level with associated exception
     * {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void debug(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(Level.DEBUG, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message;
     */
    public void debug(String msg, Throwable th) {
        log.log(Level.DEBUG, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void warn(String msg, Object... params) {
        logInternal(Level.WARNING, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#WARNING} level with associated exception
     * {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void warn(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(Level.WARNING, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void warn(String msg, Throwable th) {
        log.log(Level.WARNING, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void error(String msg, Object... params) {
        logInternal(Level.ERROR, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#ERROR} level with associated exception
     * {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void error(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(Level.ERROR, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void error(String msg, Throwable th) {
        log.log(Level.ERROR, msg, th);
    }

    /**
     * @param msg The message pattern which will be formatted and passed to the {@link System.Logger}.
     * @param params A list of arguments to be substituted in place of formatting anchors.
     */
    public void trace(String msg, Object... params) {
        logInternal(Level.TRACE, msg, params);
    }

    /**
     * Logs a message which produces in {@code msgSupplier}, on {@link Level#TRACE} level with associated exception
     * {@code thrown}.
     *
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     */
    public void trace(Supplier<String> msgSupplier, Throwable thrown) {
        logInternalExceptional(Level.TRACE, msgSupplier, thrown);
    }

    /**
     * @param msg The message pattern which will be passed to the {@link System.Logger}.
     * @param th A {@code Throwable} associated with the log message.
     */
    public void trace(String msg, Throwable th) {
        log.log(Level.TRACE, msg, th);
    }

    /**
     * Logs a message with an optional list of parameters.
     *
     * @param level One of the log message level identifiers.
     * @param msg The string message format in {@link LoggerMessageHelper} format.
     * @param params An optional list of parameters to the message (may be none).
     * @throws NullPointerException If {@code level} is {@code null}.
     */
    private void logInternal(Level level, String msg, Object... params) {
        Objects.requireNonNull(level);

        if (!log.isLoggable(level))
            return;

        log.log(level, LoggerMessageHelper.arrayFormat(msg, params));
    }

    /**
     * Logs a lazily supplied message associated with a given throwable.
     *
     * @param level One of the log message level identifiers.
     * @param msgSupplier A supplier function that produces a message.
     * @param thrown A {@code Throwable} associated with log message; can be {@code null}.
     * @throws NullPointerException If {@code level} is {@code null}, or {@code msgSupplier} is {@code null}.
     */
    private void logInternalExceptional(Level level, Supplier<String> msgSupplier, Throwable thrown) {
        Objects.requireNonNull(level);
        Objects.requireNonNull(msgSupplier);

        if (!log.isLoggable(level))
            return;

        log.log(level, msgSupplier.get(), thrown);
    }

    /**
     * @return {@code true} if the {@link Level#TRACE} log message level is currently being logged.
     */
    public boolean isTraceEnabled() {
        return log.isLoggable(Level.TRACE);
    }

    /**
     * @return {@code true} if the {@link Level#DEBUG} log message level is currently being logged.
     */
    public boolean isDebugEnabled() {
        return log.isLoggable(Level.DEBUG);
    }

    /**
     * @return {@code true} if the {@link Level#INFO} log message level is currently being logged.
     */
    public boolean isInfoEnabled() {
        return log.isLoggable(Level.INFO);
    }

    /**
     * @return {@code true} if the {@link Level#WARNING} log message level is currently being logged.
     */
    public boolean isWarnEnabled() {
        return log.isLoggable(Level.WARNING);
    }
}

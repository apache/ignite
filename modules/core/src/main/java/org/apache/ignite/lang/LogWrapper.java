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

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Wraps system logger for more convenient access.
 */
public class LogWrapper {
    /** Logger delegate. */
    private final System.Logger log;

    /**
     * @param cls The class for a logger.
     */
    public LogWrapper(Class<?> cls) {
        this.log = System.getLogger(Objects.requireNonNull(cls).getName());
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
     * @param e The exception.
     */
    public void error(String msg, Exception e) {
        log.log(ERROR, msg, e);
    }
}

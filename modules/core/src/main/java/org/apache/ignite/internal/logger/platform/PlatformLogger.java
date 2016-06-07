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

package org.apache.ignite.internal.logger.platform;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.jetbrains.annotations.Nullable;

/**
 * Logger that delegates to platform.
 */
public class PlatformLogger implements IgniteLogger {
    /** Callbacks. */
    private final PlatformCallbackGateway gate;

    /**
     * Ctor.
     *
     * @param gate Callback gateway.
     */
    public PlatformLogger(PlatformCallbackGateway gate, Object ctgr) {
        this.gate = gate;

        String cat = ctgr instanceof Class ? ((Class)ctgr).getName() : String.valueOf(ctgr);

        // TODO: Initialize instance in platform
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new PlatformLogger(gate, ctgr);
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {

    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {

    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {

    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {

    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {

    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {

    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {

    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return null;
    }
}

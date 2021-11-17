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

package org.apache.ignite.logger;

import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Logger which does not output anything.
 */
public class NullLogger extends IgniteLogger {
    /**
     * Creates null logger.
     */
    public NullLogger() {
        super(NullLogger.class);
    }
    
    
    /** {@inheritDoc} */
    @Override
    public void info(String msg, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void info(String msg, Throwable th, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void info(Supplier<String> msgSupplier, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void info(String msg, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void debug(String msg, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void debug(String msg, Throwable th, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void debug(Supplier<String> msgSupplier, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void debug(String msg, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void warn(String msg, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void warn(String msg, Throwable th, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void warn(Supplier<String> msgSupplier, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void warn(String msg, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void error(String msg, @Nullable Throwable e) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void error(String msg, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void error(String msg, Throwable th, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void error(Supplier<String> msgSupplier, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void trace(String msg, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void trace(String msg, Throwable th, Object... params) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void trace(Supplier<String> msgSupplier, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public void trace(String msg, Throwable th) {
        // No-op.
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isTraceEnabled() {
        return false;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isDebugEnabled() {
        return false;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isInfoEnabled() {
        return false;
    }
}

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

package org.apache.ignite.internal.logger;

import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Internal extension of {@link IgniteLogger}.
 */
public interface IgniteLoggerEx extends IgniteLogger {
    /**
     * Adds console appender to the logger.
     * @param clearOutput If {@code true} then console output must be configured without any additional info
     *                    like time, message level, thread info, etc.
     */
    public void addConsoleAppender(boolean clearOutput);

    /** Flush any buffered output. */
    public void flush();

    /**
     * Sets application name and node ID.
     *
     * @param application Application.
     * @param nodeId Node ID.
     */
    public void setApplicationAndNode(@Nullable String application, @Nullable UUID nodeId);
}

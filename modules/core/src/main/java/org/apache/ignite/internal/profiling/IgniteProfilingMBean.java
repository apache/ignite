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

package org.apache.ignite.internal.profiling;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParameter;

/**
 * MBean provide access to profiling management.
 */
@MXBeanDescription("MBean provide access to profiling management.")
public interface IgniteProfilingMBean {
    /**
     * Start profiling in the cluster with default settings.
     *
     * @see LogFileProfiling#DFLT_FILE_MAX_SIZE
     * @see LogFileProfiling#DFLT_BUFFER_SIZE
     * @see LogFileProfiling#DFLT_FLUSH_SIZE
     */
    @MXBeanDescription("Start profiling in the cluster.")
    public void startProfiling() throws IgniteCheckedException;

    /** Start profiling in the cluster. */
    @MXBeanDescription("Start profiling in the cluster.")
    public void startProfiling(
        @MXBeanParameter(name = "maxFileSize", description = "Maximum file size in bytes.") long maxFileSize,
        @MXBeanParameter(name = "bufferSize", description = "Off heap buffer size in bytes.") int bufferSize,
        @MXBeanParameter(name = "flushBatchSize", description = "Minimal batch size to flush in bytes.") int flushBatchSize)
        throws IgniteCheckedException;

    /** Stop profiling in the cluster. */
    @MXBeanDescription("Stop profiling in the cluster.")
    public void stopProfiling() throws IgniteCheckedException;

    /** @return {@code True} if profiling enabled. */
    @MXBeanDescription("True if profiling enabled.")
    public boolean profilingEnabled();
}

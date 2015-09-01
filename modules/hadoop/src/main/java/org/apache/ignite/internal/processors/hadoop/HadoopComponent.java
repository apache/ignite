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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;

/**
 * Abstract class for all hadoop components.
 */
public abstract class HadoopComponent {
    /** Hadoop context. */
    protected HadoopContext ctx;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * @param ctx Hadoop context.
     */
    public void start(HadoopContext ctx) throws IgniteCheckedException {
        this.ctx = ctx;

        log = ctx.kernalContext().log(getClass());
    }

    /**
     * Stops manager.
     */
    public void stop(boolean cancel) {
        // No-op.
    }

    /**
     * Callback invoked when all grid components are started.
     */
    public void onKernalStart() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Callback invoked before all grid components are stopped.
     */
    public void onKernalStop(boolean cancel) {
        // No-op.
    }
}
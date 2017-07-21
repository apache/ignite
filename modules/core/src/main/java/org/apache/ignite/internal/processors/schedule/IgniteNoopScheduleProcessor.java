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

package org.apache.ignite.internal.processors.schedule;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.scheduler.SchedulerFuture;

/**
 * No-op implementation of {@link IgniteScheduleProcessorAdapter}, throws exception on usage attempt.
 */
public class IgniteNoopScheduleProcessor extends IgniteScheduleProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public IgniteNoopScheduleProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(Runnable c, String pattern) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        throw processorException();
    }

    /**
     * @return No-op processor usage exception;
     */
    private IgniteException processorException() {
        return new IgniteException("Current Ignite configuration does not support schedule functionality " +
            "(consider adding ignite-schedule module to classpath).");
    }
}
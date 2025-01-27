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

package org.apache.ignite.internal.processors.query.calcite.exec.task;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;

/**
 * Abstract query task executor.
 */
public abstract class AbstractQueryTaskExecutor extends AbstractService implements QueryTaskExecutor, Thread.UncaughtExceptionHandler {
    /** */
    public static final String THREAD_POOL_NAME = "CalciteQueryExecutor";

    /** */
    protected final GridKernalContext ctx;

    /** */
    protected Thread.UncaughtExceptionHandler eHnd;

    /** */
    protected AbstractQueryTaskExecutor(GridKernalContext ctx) {
        super(ctx);
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void uncaughtException(Thread t, Throwable e) {
        if (eHnd != null)
            eHnd.uncaughtException(t, e);
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.exec;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;

/**
 * Timeout service implementation.
 */
public class TimeoutServiceImpl extends AbstractService implements TimeoutService {
    /** */
    private final GridTimeoutProcessor proc;

    /** */
    public TimeoutServiceImpl(GridKernalContext ctx) {
        super(ctx);

        proc = ctx.timeout();
    }

    /** {@inheritDoc} */
    @Override public void schedule(Runnable task, long timeout) {
        proc.schedule(task, timeout, -1);
    }
}

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

package org.apache.ignite.internal;

import java.util.Collections;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionsTask;
import org.apache.ignite.internal.visor.compute.VisorComputeCancelSessionsTaskArg;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;

/**
 * ComputeMXBean implementation.
 */
public class ComputeMXBeanImpl implements ComputeMXBean {
    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public ComputeMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void cancel(String sessionId) {
        A.notNull(sessionId, "sessionId");

        cancel(IgniteUuid.fromString(sessionId));
    }

    /**
     * Kills compute task by the session idenitifier.
     *
     * @param sessionId Session id.
     */
    public void cancel(IgniteUuid sessionId) {
        VisorComputeCancelSessionsTaskArg arg = new VisorComputeCancelSessionsTaskArg(
            Collections.singleton(sessionId));

        try {
            IgniteCompute compute = ctx.cluster().get().compute();

            compute.execute(new VisorComputeCancelSessionsTask(),
                new VisorTaskArgument<>(ctx.cluster().get().localNode().id(), arg, false));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

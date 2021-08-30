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

import java.util.Map;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * ComputeMXBean implementation.
 */
public class ComputeMXBeanImpl implements ComputeMXBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public ComputeMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void cancel(String sesId) {
        A.notNull(sesId, "sessionId");

        cancel(IgniteUuid.fromString(sesId));
    }

    /**
     * Kills compute task by the session idenitifier.
     *
     * @param sesId Session id.
     */
    public void cancel(IgniteUuid sesId) {
        try {
            ctx.grid().compute(ctx.grid().cluster()).broadcast(new ComputeCancelSession(), sesId);
        }
        catch (IgniteException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Cancel compute session closure.
     */
    private static class ComputeCancelSession implements IgniteClosure<IgniteUuid, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(IgniteUuid sesId) {
            ignite.context().job().cancelJob(sesId, null, false);

            IgniteCompute compute = ignite.compute(ignite.cluster().forLocal());

            Map<IgniteUuid, ComputeTaskFuture<Object>> futs = compute.activeTaskFutures();

            ComputeTaskFuture<Object> fut = futs.get(sesId);

            if (fut != null)
                fut.cancel();

            return null;
        }
    }
}

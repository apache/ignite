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

import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTaskArg;
import org.apache.ignite.mxbean.QueryMXBean;

/**
 * QueryMXBean implementation.
 */
public class QueryMXBeanImpl implements QueryMXBean {
    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public QueryMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void cancelContinuous(String id) {
        A.notNull(id, "id");

        ctx.continuous().stopRoutine(UUID.fromString(id));
    }

    /** {@inheritDoc} */
    @Override public void cancelSQL(Long id) {
        A.notNull(id, "id");

        VisorQueryCancelTaskArg arg = new VisorQueryCancelTaskArg(id);

        try {
            IgniteCompute compute = ctx.cluster().get().compute();

            compute.execute(new VisorQueryCancelTask(),
                new VisorTaskArgument<>(ctx.cluster().get().localNode().id(), arg, false));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelScan(Long id) {
        A.notNull(id, "id");

    }
}

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

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTask;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTaskArg;
import org.apache.ignite.mxbean.ServiceMXBean;

/**
 * ServiceMXBean implementation.
 */
public class ServiceMXBeanImpl implements ServiceMXBean {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public ServiceMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(ServiceMXBeanImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void cancel(String name) {
        A.notNull(name, "name");

        if (log.isInfoEnabled())
            log.info("Canceling service[name=" + name + ']');

        boolean res;

        try {
            IgniteClusterImpl cluster = ctx.cluster().get();

            IgniteCompute compute = cluster.compute();

            res = compute.execute(new VisorCancelServiceTask(),
                new VisorTaskArgument<>(ctx.localNodeId(), new VisorCancelServiceTaskArg(name), false));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!res)
            throw new RuntimeException("Service not found or can't be canceled[name=" + name + ']');
    }
}

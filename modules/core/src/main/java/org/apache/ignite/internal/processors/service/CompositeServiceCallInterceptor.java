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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.Callable;
import org.apache.ignite.services.ServiceCallInterceptor;
import org.apache.ignite.services.ServiceContext;

/**
 * Composite service call interceptor.
 */
public class CompositeServiceCallInterceptor implements ServiceCallInterceptor {
    /** */
    private final ServiceCallInterceptor[] intcps;

    /** */
    public CompositeServiceCallInterceptor(ServiceCallInterceptor[] intcps) {
        this.intcps = intcps;
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String mtd, Object[] args, ServiceContext ctx, Callable<Object> svcCall) throws Exception {
        return new Callable<Object>() {
            int idx;

            @Override public Object call() throws Exception {
                return intcps[idx].invoke(mtd, args, ctx, ++idx == intcps.length ? svcCall : this);
            }
        }.call();
    }
}

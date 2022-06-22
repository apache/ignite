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

package org.apache.ignite.services;

import java.io.Serializable;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Service method invocation interceptor.
 *
 * @see ServiceInterceptorContext
 * @see ServiceCallContext
 */
@IgniteExperimental
public interface ServiceCallInterceptor extends Serializable {
    /**
     * Executes before service method invocation.
     *
     * @param ctx Mutable service call context.
     * @throws ServiceInterceptException If failed.
     */
    public default void onInvoke(ServiceInterceptorContext ctx) throws ServiceInterceptException {
        // No-op.
    }

    /**
     * Executes after method has completed successfully.
     *
     * @param res Method result.
     * @param ctx Mutable service call context.
     * @throws ServiceInterceptException If failed.
     */
    public default void onComplete(@Nullable Object res, ServiceInterceptorContext ctx) throws ServiceInterceptException {
        // No-op.
    }

    /**
     * Executes when a method invocation error occurs,
     *
     * @param err Invocation exception.
     * @param ctx Mutable service call context.
     */
    public default void onError(Throwable err, ServiceInterceptorContext ctx) {
        // No-op.
    }
}

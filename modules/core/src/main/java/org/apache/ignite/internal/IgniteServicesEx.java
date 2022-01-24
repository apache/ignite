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
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.jetbrains.annotations.Nullable;

/**
 * Extended interface that provides additional internal methods for managing services.
 */
public interface IgniteServicesEx extends IgniteServices {
    /**
     * Gets a remote handle on the service. If service is available locally and no caller context provider is
     * specified, then a local instance is returned and the timeout is ignored, otherwise, a proxy is dynamically
     * created and provided for the specified service.
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @param sticky Whether or not Ignite should always contact the same remote
     *      service or try to load-balance between services.
     * @param callAttrsProvider Service call context attributes provider.
     * @param timeout If greater than 0 created proxy will wait for service availability only specified time,
     *  and will limit remote service invocation time.
     * @param <T> Service type.
     * @return Either proxy over remote service or local service if it is deployed locally and no caller context
     *         provider is specified.
     * @throws IgniteException If failed to create service proxy.
     */
    public <T> T serviceProxy(
        String name,
        Class<? super T> svcItf,
        boolean sticky,
        @Nullable Supplier<Map<String, Object>> callAttrsProvider,
        long timeout
    ) throws IgniteException;
}

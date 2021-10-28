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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.services.ServiceCallContext;
import org.jetbrains.annotations.Nullable;

/**
 * Service call context implementation.
 */
public class ServiceCallContextImpl implements ServiceCallContext {
    /** Service call context of the current thread. */
    private static final ThreadLocal<ServiceCallContext> locCallCtx = new ThreadLocal<>();

    /** Read-only empty context. */
    private static final ServiceCallContext emptyCtx = new ServiceCallContextImpl(Collections.emptyMap());

    /** Service call context attributes. */
    private final Map<String, Object> attrs;

    /**
     * Default contructor.
     */
    public ServiceCallContextImpl() {
        attrs = new HashMap<>();
    }

    /**
     * @param attrs Service call context attributes.
     */
    public ServiceCallContextImpl(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @Override public String attribute(String name) {
        return (String)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public byte[] binary(String name) {
        return (byte[])attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ServiceCallContext put(String name, String val) {
        A.notNullOrEmpty(name, "name");

        attrs.put(name, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public ServiceCallContext put(String name, byte[] val) {
        A.notNullOrEmpty(name, "name");

        attrs.put(name, val);

        return this;
    }

    /**
     * @return Service call context attributes.
     */
    protected Map<String, Object> values() {
        return attrs;
    }

    /**
     * @return Service call context of the current thread.
     */
    public static ServiceCallContext current() {
        ServiceCallContext callCtx = locCallCtx.get();

        return callCtx == null ? emptyCtx : callCtx;
    }

    /**
     * @param callCtx Service call context of the current thread.
     */
    protected static void current(@Nullable ServiceCallContext callCtx) {
        if (callCtx != null)
            locCallCtx.set(callCtx);
        else
            locCallCtx.remove();
    }
}

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

import java.util.Map;
import org.apache.ignite.services.ServiceInterceptorContext;
import org.jetbrains.annotations.Nullable;

/**
 * Service call context implementation.
 */
public class ServiceCallContextImpl implements ServiceInterceptorContext {
    /** Method name. */
    private final String mtdName;

    /** Method arguments. */
    private final Object[] mtdArgs;

    /** Service call context attributes. */
    private final Map<String, Object> attrs;

    /**
     * @param attrs Service call context attributes.
     * @param mtdName Method name.
     * @param mtdArgs Method arguments.
     */
    public ServiceCallContextImpl(Map<String, Object> attrs, String mtdName, Object[] mtdArgs) {
        this.attrs = attrs;
        this.mtdName = mtdName;
        this.mtdArgs = mtdArgs;
    }

    /** {@inheritDoc} */
    @Override public String attribute(String name) {
        return (String)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public void attribute(String name, String val) {
        attrs.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public byte[] binaryAttribute(String name) {
        return (byte[])attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public void binaryAttribute(String name, byte[] val) {
        attrs.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public String method() {
        return mtdName;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object[] arguments() {
        return mtdArgs;
    }

    /**
     * @return Service call context attributes.
     */
    public Map<String, Object> values() {
        return attrs;
    }
}

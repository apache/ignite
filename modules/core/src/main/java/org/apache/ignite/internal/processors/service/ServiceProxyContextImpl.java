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
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.services.ServiceProxyContext;

/**
 * Service proxy context.
 */
public class ServiceProxyContextImpl extends ServiceProxyContext {
    /** Context attributes. */
    private final Map<String, Object> attrs;

    /**
     * @param attrs Context attributes.
     */
    public ServiceProxyContextImpl(Map<String, Object> attrs) {
        A.notNull(attrs, "attrs");
        A.ensure(!attrs.isEmpty(), "cannot create an empty context.");

        this.attrs = attrs;
    }

    /**
     * Constructor to create an empty context.
     */
    public ServiceProxyContextImpl() {
        attrs = Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public <V> V attribute(String name) {
        return (V)attrs.get(name);
    }

    /**
     * @return Context attributes.
     */
    protected Map<String, Object> values() {
        return attrs;
    }

    /**
     * Set threadl local context.
     *
     * @param attrs Service operation context.
     */
    protected static void current(Map<String, Object> attrs) {
        if (attrs != null)
            threadProxyCtx.set(new ServiceProxyContextImpl(attrs));
        else
            threadProxyCtx.remove();
    }
}

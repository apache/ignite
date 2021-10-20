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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.service.ServiceProxyContextImpl;

/**
 * Service proxy context builder.
 */
public class ServiceProxyContextBuilder {
    /** Context attributes. */
    private final Map<String, Object> values;

    /**
     * Default contructor.
     */
    public ServiceProxyContextBuilder() {
        values = new HashMap<>();
    }

    /**
     * @param values Context attributes.
     */
    public ServiceProxyContextBuilder(Map<String, Object> values) {
        this.values = new HashMap<>(values);
    }

    /**
     * @param name Operation context attribute name.
     * @param val Operation context attribute value.
     */
    public ServiceProxyContextBuilder(String name, Object val) {
        this();

        put(name, val);
    }

    /**
     * @param name Context attribute name.
     * @param val Context attribute value.
     * @return This for chaining.
     */
    public ServiceProxyContextBuilder put(String name, Object val) {
        values.put(name, val);

        return this;
    }

    /**
     * @return Service proxy context.
     */
    public ServiceProxyContext build() {
        return new ServiceProxyContextImpl(values);
    }
}

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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.service.ServiceCallContextImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Service call context builder.
 */
@IgniteExperimental
public class ServiceCallContextBuilder {
    /** Service call context attributes. */
    private final Map<String, byte[]> attrs = new HashMap<>();

    /**
     * Put string attribute.
     *
     * @param name Attribute name.
     * @param value Attribute value.
     * @return This for chaining.
     */
    public ServiceCallContextBuilder put(String name, String value) {
        A.notNullOrEmpty(name, "name");
        A.notNull(value, "value");

        attrs.put(name, value.getBytes(StandardCharsets.UTF_8));

        return this;
    }

    /**
     * Put binary attribute.
     *
     * @param name Attribute name.
     * @param value Attribute value.
     * @return This for chaining.
     */
    public ServiceCallContextBuilder put(String name, byte[] value) {
        A.notNullOrEmpty(name, "name");
        A.notNull(value, "value");

        attrs.put(name, value);

        return this;
    }

    /**
     * @return Service call context.
     */
    public ServiceCallContext build() {
        if (attrs.isEmpty())
            throw new IllegalStateException("Cannot create an empty context.");

        return new ServiceCallContextImpl(attrs);
    }
}

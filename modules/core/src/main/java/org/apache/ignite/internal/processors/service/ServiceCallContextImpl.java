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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceCallContext;

/**
 * Service call context implementation.
 */
public class ServiceCallContextImpl implements ServiceCallContext {
    /** Service call context attributes. */
    private Map<String, byte[]> attrs;

    /**
     * Default contructor.
     */
    public ServiceCallContextImpl() {
        attrs = new HashMap<>();
    }

    /**
     * Constructs an immutable context from the map.
     *
     * @param attrs Service call context attributes.
     */
    protected ServiceCallContextImpl(Map<String, byte[]> attrs) {
        this.attrs = Collections.unmodifiableMap(attrs);
    }

    /** {@inheritDoc} */
    @Override public String attribute(String name) {
        byte[] bytes = attrs.get(name);

        if (bytes == null)
            return null;

        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** {@inheritDoc} */
    @Override public byte[] binaryAttribute(String name) {
        return attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ServiceCallContext put(String name, String value) {
        A.notNullOrEmpty(name, "name");
        A.notNull(value, "value");

        attrs.put(name, value.getBytes(StandardCharsets.UTF_8));

        return this;
    }

    /** {@inheritDoc} */
    @Override public ServiceCallContext put(String name, byte[] value) {
        A.notNullOrEmpty(name, "name");
        A.notNull(value, "value");

        attrs.put(name, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public ServiceCallContext copy() {
        return new ServiceCallContextImpl(attrs);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeMap(out, attrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        attrs = Collections.unmodifiableMap(U.readMap(in));
    }
}

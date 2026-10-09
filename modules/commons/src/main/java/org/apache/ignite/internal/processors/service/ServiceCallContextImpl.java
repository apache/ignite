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
import org.apache.ignite.services.ServiceCallContext;

/**
 * Service call context implementation.
 */
public class ServiceCallContextImpl implements ServiceCallContext {
    /** Service call context attributes. */
    private Map<String, Object> attrs;

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
    @Override public byte[] binaryAttribute(String name) {
        return (byte[])attrs.get(name);
    }

    /**
     * @return Service call context attributes.
     */
    public Map<String, Object> values() {
        return attrs;
    }
}

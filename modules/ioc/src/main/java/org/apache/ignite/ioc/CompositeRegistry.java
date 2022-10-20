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
package org.apache.ignite.ioc;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;

/**
 * Implementation of registry which can combine many sources of beans.
 * <p>
 * Lookups are ordered in passing order of registries, so first one returning non-null value will
 * drive the result.
 */
public class CompositeRegistry implements Registry {
    /** Resource registries. */
    private final List<Registry> registries;

    /**
     * Constructor.
     *
     * @param registries Resource registries.
     */
    public CompositeRegistry(List<Registry> registries) {
        this.registries = registries;
    }

    /** {@inheritDoc} */
    @Override public <T> T lookup(Class<T> type) {
        for (Registry registry : registries) {
            T bean = registry.lookup(type);
            if (bean != null) {
                return bean;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object lookup(String name) {
        for (Registry registry : registries) {
            Object bean = registry.lookup(name);
            if (bean != null) {
                return bean;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTarget(Object target) throws IgniteCheckedException {
        Object bean = target;
        for (Registry registry : registries) {
            bean = registry.unwrapTarget(target);
        }
        return bean;
    }
}

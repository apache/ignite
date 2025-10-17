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

package org.apache.ignite.internal.thread.context;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** */
public class ThreadContextAttributeRegistry {
    /** */
    private static final ThreadContextAttributeRegistry INSTANCE = new ThreadContextAttributeRegistry();

    /** */
    private final List<ThreadContextAttribute<?>> attrs = new CopyOnWriteArrayList<>();

    /**
     * Registers attribute with initial value set to {@code null}.
     *
     * @see #register(Object)
     */
    public <T> ThreadContextAttribute<T> register() {
        return register(null);
    }

    /**
     * Registers attribute with specified initial value. Initial value is returned by
     * {@link ThreadContext#get(ThreadContextAttribute)} method if attribute value is not set explicitly.
     * Returned value represents a key used to access attribute value via {@link ThreadContext#get(ThreadContextAttribute)}
     * or {@link ThreadContext#withAttribute(ThreadContextAttribute, Object)} methods.
     *
     * @param initialVal Attribute initial value.
     * @return Registered attribute instance.
     */
    public synchronized <T> ThreadContextAttribute<T> register(T initialVal) {
        ThreadContextAttribute<T> attr = new ThreadContextAttribute<>(attrs.size(), initialVal);

        attrs.add(attr);

        return attr;
    }

    /** */
    <T> ThreadContextAttribute<T> attribute(int id) {
        return (ThreadContextAttribute<T>)attrs.get(id);
    }

    /** */
    int size() {
        return attrs.size();
    }

    /** */
    public static ThreadContextAttributeRegistry instance() {
        return INSTANCE;
    }
}

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

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.Nullable;

/**
 * Service context implementation.
 */
public class ServiceContextImpl implements ServiceContext {
    /** */
    private static final long serialVersionUID = 0L;

    /** Null method. */
    private static final Method NULL_METHOD = ServiceContextImpl.class.getMethods()[0];

    /** Service name. */
    private final String name;

    /** Execution ID. */
    private final UUID execId;

    /** Cache name. */
    private final String cacheName;

    /** Affinity key. */
    private final Object affKey;

    /** Service. */
    @GridToStringExclude
    private final Service svc;

    /** Executor service. */
    @GridToStringExclude
    private final ExecutorService exe;

    /** Methods reflection cache. */
    private final ConcurrentMap<GridServiceMethodReflectKey, Method> mtds = new ConcurrentHashMap<>();

    /** Cancelled flag. */
    private volatile boolean isCancelled;


    /**
     * @param name Service name.
     * @param execId Execution ID.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param svc Service.
     * @param exe Executor service.
     */
    ServiceContextImpl(String name, UUID execId, String cacheName, Object affKey, Service svc,
        ExecutorService exe) {
        this.name = name;
        this.execId = execId;
        this.cacheName = cacheName;
        this.affKey = affKey;
        this.svc = svc;
        this.exe = exe;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public UUID executionId() {
        return execId;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return isCancelled;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <K> K affinityKey() {
        return (K)affKey;
    }

    /**
     * @return Service instance.
     */
    Service service() {
        return svc;
    }

    /**
     * @return Executor service.
     */
    ExecutorService executor() {
        return exe;
    }

    /**
     * @param key Method key.
     * @return Method.
     */
    @Nullable Method method(GridServiceMethodReflectKey key) {
        Method mtd = mtds.get(key);

        if (mtd == null) {
            try {
                mtd = svc.getClass().getMethod(key.methodName(), key.argTypes());
            }
            catch (NoSuchMethodException e) {
                mtd = NULL_METHOD;
            }

            mtds.put(key, mtd);
        }

        return mtd == NULL_METHOD ? null : mtd;
    }

    /**
     * @param isCancelled Cancelled flag.
     */
    public void setCancelled(boolean isCancelled) {
        this.isCancelled = isCancelled;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceContextImpl.class, this);
    }
}
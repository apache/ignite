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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

/**
 * Cache start descriptor.
 */
public class DynamicCacheDescriptor {
    /** Cache start ID. */
    private IgniteUuid deploymentId;

    /** Cache configuration. */
    @GridToStringExclude
    private CacheConfiguration cacheCfg;

    /** Cancelled flag. */
    private boolean cancelled;

    /** Validation error. */
    private IgniteCheckedException validationError;

    /** Locally configured flag. */
    private boolean locCfg;

    /** Statically configured flag. */
    private boolean staticCfg;

    /** Started flag. */
    private boolean started;

    /**
     * @param cacheCfg Cache configuration.
     */
    public DynamicCacheDescriptor(CacheConfiguration cacheCfg, IgniteUuid deploymentId) {
        this.cacheCfg = cacheCfg;
        this.deploymentId = deploymentId;
    }

    /**
     * @return Start ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @param deploymentId Deployment ID.
     */
    public void deploymentId(IgniteUuid deploymentId) {
        this.deploymentId = deploymentId;
    }

    /**
     * @return Locally configured flag.
     */
    public boolean locallyConfigured() {
        return locCfg;
    }

    /**
     * @param locCfg Locally configured flag.
     */
    public void locallyConfigured(boolean locCfg) {
        this.locCfg = locCfg;
    }

    /**
     * @return {@code True} if statically configured.
     */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /**
     * @param staticCfg {@code True} if statically configured.
     */
    public void staticallyConfigured(boolean staticCfg) {
        this.staticCfg = staticCfg;
    }

    /**
     * @return {@code True} if started flag was flipped by this call.
     */
    public boolean onStart() {
        if (!started) {
            started = true;

            return true;
        }

        return false;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * Sets cancelled flag.
     */
    public void onCancelled() {
        cancelled = true;
    }

    /**
     * @return Cancelled flag.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @return {@code True} if descriptor is valid and cache should be started.
     */
    public boolean valid() {
        return validationError == null;
    }

    /**
     * @throws IgniteCheckedException If validation failed.
     */
    public void checkValid() throws IgniteCheckedException {
        if (validationError != null)
            throw validationError;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", cacheCfg.getName());
    }

    /**
     * Sets validation error.
     *
     * @param e Validation error.
     */
    public void validationFailed(IgniteCheckedException e) {
        validationError = e;
    }
}

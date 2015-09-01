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

package org.apache.ignite.internal.processors.cache.store;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.marshaller.portable.PortableMarshaller;

/**
 * Default store manager implementation.
 */
public class CacheOsStoreManager extends GridCacheStoreManagerAdapter {
    /** Ignite context. */
    private final GridKernalContext ctx;

    /** Cache configuration. */
    private final CacheConfiguration cfg;

    /**
     * Constructor.
     *
     * @param ctx Ignite context.
     * @param cfg Cache configuration.
     */
    public CacheOsStoreManager(GridKernalContext ctx, CacheConfiguration cfg) {
        this.ctx = ctx;
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridKernalContext igniteContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean convertPortable() {
        return !(cfg.isKeepPortableInStore() && ctx.config().getMarshaller() instanceof PortableMarshaller);
    }
}
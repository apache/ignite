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

package org.apache.ignite.internal.processors.platform.plugin.cache;

import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;

/**
 * Platform cache plugin configuration.
 */
public class PlatformCachePluginConfiguration implements CachePluginConfiguration {
    /** */
    private static final long serialVersionUID = 0L;

    /** Native configuration object. */
    private final Object nativeCfg;

    /**
     * Ctor.
     *
     * @param nativeCfg Native configuration object.
     */
    public PlatformCachePluginConfiguration(Object nativeCfg) {
        assert nativeCfg != null;

        this.nativeCfg = nativeCfg;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createProvider(CachePluginContext ctx) {
        return new PlatformCachePluginProvider(ctx, nativeCfg);
    }

    /**
     * Gets the native configuration object.
     *
     * @return Native configuration object.
     */
    public Object nativeCfg() {
        return nativeCfg;
    }
}
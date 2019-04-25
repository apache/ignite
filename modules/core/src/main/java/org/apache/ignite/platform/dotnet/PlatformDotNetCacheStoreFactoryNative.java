/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.platform.dotnet;

import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetCacheStore;

import javax.cache.configuration.Factory;

/**
 * Cache store factory that wraps native factory object.
 */
public class PlatformDotNetCacheStoreFactoryNative implements Factory<PlatformDotNetCacheStore> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Object nativeFactory;

    /**
     * Ctor.
     *
     * @param nativeFactory Native factory object.
     */
    public PlatformDotNetCacheStoreFactoryNative(Object nativeFactory) {
        assert nativeFactory != null;

        this.nativeFactory = nativeFactory;
    }

    /**
     * Gets the wrapped factory object.
     *
     * @return Factory object.
     */
    public Object getNativeFactory() {
        return nativeFactory;
    }

    /** {@inheritDoc} */
    @Override public PlatformDotNetCacheStore create() {
        return new PlatformDotNetCacheStore(nativeFactory);
    }
}

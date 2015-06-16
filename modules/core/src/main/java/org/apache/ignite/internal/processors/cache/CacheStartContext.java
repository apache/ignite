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

import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.expiry.*;
import java.util.*;

/**
 * Cache start context.
 */
public class CacheStartContext {
    /** */
    private CacheStore store;

    /** */
    private ExpiryPolicy expPlc;

    /** */
    private Collection<CacheStoreSessionListener> sesLsnrs;

    /**
     * @param cfg Configuration.
     */
    public CacheStartContext(GridKernalContext ctx, CacheConfiguration<?, ?> cfg) {
        assert ctx != null;
        assert cfg != null;

        store = CU.create(ctx, cfg.getCacheStoreFactory());
        expPlc = CU.create(ctx, cfg.getExpiryPolicyFactory());
        sesLsnrs = CU.create(ctx, cfg.getCacheStoreSessionListenerFactories());
    }

    /**
     * @return Cache store.
     */
    public CacheStore store() {
        return store;
    }

    /**
     * @return Expiry policy.
     */
    public ExpiryPolicy expiryPolicy() {
        return expPlc;
    }

    /**
     * @return Store session listeners.
     */
    public Collection<CacheStoreSessionListener> storeSessionListeners() {
        return sesLsnrs;
    }
}

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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for cache store configuration properties.
 */
public class VisorCacheStoreConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache store. */
    private String store;

    /** Should value bytes be stored. */
    private boolean valBytes;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache store configuration properties.
     */
    public static VisorCacheStoreConfiguration from(CacheConfiguration ccfg) {
        VisorCacheStoreConfiguration cfg = new VisorCacheStoreConfiguration();

        cfg.store(compactClass(ccfg.getCacheStoreFactory()));
        cfg.valueBytes(ccfg.isStoreValueBytes());

        return cfg;
    }

    public boolean enabled() {
        return store != null;
    }

    /**
     * @return Cache store.
     */
    @Nullable public String store() {
        return store;
    }

    /**
     * @param store New cache store.
     */
    public void store(String store) {
        this.store = store;
    }

    /**
     * @return Should value bytes be stored.
     */
    public boolean valueBytes() {
        return valBytes;
    }

    /**
     * @param valBytes New should value bytes be stored.
     */
    public void valueBytes(boolean valBytes) {
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheStoreConfiguration.class, this);
    }
}

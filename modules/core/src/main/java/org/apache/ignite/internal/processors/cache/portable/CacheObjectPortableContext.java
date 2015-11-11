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

package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;

/**
 *
 */
public class CacheObjectPortableContext extends CacheObjectContext {
    /** */
    private boolean portableEnabled;

    /**
     * @param kernalCtx Kernal context.
     * @param portableEnabled Portable enabled flag.
     * @param cpyOnGet Copy on get flag.
     * @param storeVal {@code True} if should store unmarshalled value in cache.
     * @param depEnabled {@code true} if deployment is enabled for the given cache.
     */
    public CacheObjectPortableContext(GridKernalContext kernalCtx,
        boolean cpyOnGet,
        boolean storeVal,
        boolean portableEnabled,
        boolean depEnabled) {
        super(kernalCtx, portableEnabled ? new CacheDefaultPortableAffinityKeyMapper() :
            new GridCacheDefaultAffinityKeyMapper(), cpyOnGet, storeVal, depEnabled);

        this.portableEnabled = portableEnabled;
    }

    /**
     * @return Portable enabled flag.
     */
    public boolean portableEnabled() {
        return portableEnabled;
    }
}

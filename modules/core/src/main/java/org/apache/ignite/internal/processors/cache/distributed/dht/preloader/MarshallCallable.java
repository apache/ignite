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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.Callable;

/**
 * Callable to marshall object into byte[].
 */
public class MarshallCallable implements Callable<byte[]> {
    /** GridCacheSharedContext reference. */
    private final GridCacheSharedContext gridCacheSharedContext;

    /** Payload to marshall. */
    private final Object payload;

    /** Flag to compress payload. */
    private final boolean compress;

    /**
     *
     * @param gridCacheSharedContext CridCacheSharedContext.
     * @param payload Object to marshal.
     * @param compress Flag whether compression is enabled or not.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public MarshallCallable(GridCacheSharedContext gridCacheSharedContext, Object payload, boolean compress) {
        this.gridCacheSharedContext = gridCacheSharedContext;
        this.payload = payload;
        this.compress = compress;
    }

    /** @{inheritDoc} */
    @Override public byte[] call() throws Exception {
        byte[] marshalled = U.marshal(gridCacheSharedContext, payload);

        if(compress)
            marshalled = U.zip(marshalled, gridCacheSharedContext.gridConfig().getNetCompressionLevel());

        return marshalled;
    }
}

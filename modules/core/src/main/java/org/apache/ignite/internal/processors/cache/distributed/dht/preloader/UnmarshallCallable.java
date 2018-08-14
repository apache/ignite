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
 * Callable to perform unmarshal operation.
 * @param <T> Unmarshallable class.
 */
public class UnmarshallCallable<T> implements Callable<T> {
    /** GridCacheSharedContext reference. */
    private final GridCacheSharedContext gridCacheSharedContext;

    /** Flag to compress payload. */
    private final boolean compressed;

    /** Payload to unmarshal. */
    private final byte[] payload;

    /** ClassLoader. */
    private final ClassLoader classLoader;

    /**
     *
     * @param gridCacheSharedContext GridCacheSharedContext.
     * @param payload Binary payload.
     * @param compressed Flag whether payload is compressed or not.
     * @param classLoader Class loader
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public UnmarshallCallable(
            GridCacheSharedContext gridCacheSharedContext,
            byte[] payload,
            boolean compressed,
            ClassLoader classLoader
    ) {
        this.gridCacheSharedContext = gridCacheSharedContext;
        this.payload = payload;
        this.compressed = compressed;
        this.classLoader = classLoader;
    }

    /** @{inheritDoc}  */
    @Override public T call() throws Exception {
        return compressed
                ? U.unmarshalZip(gridCacheSharedContext.marshaller(), payload, classLoader)
                : U.unmarshal(gridCacheSharedContext, payload, classLoader);
    }
}

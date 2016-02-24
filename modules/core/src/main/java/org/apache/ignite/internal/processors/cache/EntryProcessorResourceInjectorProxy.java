package org.apache.ignite.internal.processors.cache;

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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;

/**
 * @param <K>
 * @param <V>
 * @param <T>
 */
public class EntryProcessorResourceInjectorProxy<K, V, T> implements EntryProcessor<K, V, T>, Serializable {
    /** Annotations supported. */
    @SuppressWarnings("unchecked")
    public static final Class<? extends Annotation>[] annotationsSupported = new Class[] {
        CacheNameResource.class,

        SpringApplicationContextResource.class,
        SpringResource.class,
        IgniteInstanceResource.class,
        LoggerResource.class,
        ServiceResource.class
    };

    /** Delegate. */
    private EntryProcessor<K, V, T> delegate;

    /** Injected. */
    private transient boolean injected;

    /**
     * @param delegate Delegate.
     */
    public EntryProcessorResourceInjectorProxy(EntryProcessor<K, V, T> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public T process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
        if (!injected) {
            GridCacheContext cctx = entry.unwrap(GridCacheContext.class);
            GridResourceProcessor rsrcProcessor = cctx.kernalContext().resource();
            try {
                rsrcProcessor.injectGeneric(delegate);

                rsrcProcessor.injectCacheName(delegate, cctx.name());
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
            injected = true;
        }

        return delegate.process(entry, arguments);
    }
}

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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.resource.GridResourceIoc;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * @param <K>
 * @param <V>
 * @param <T>
 */
public class EntryProcessorResourceInjectorProxy<K, V, T> implements EntryProcessor<K, V, T>, Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -5032286375274064774L;

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
                rsrcProcessor.inject(delegate, GridResourceIoc.AnnotationSet.ENTRY_PROCESSOR, cctx.name());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            injected = true;
        }

        return delegate.process(entry, arguments);
    }

    /** */
    public EntryProcessor<K, V, T> getDelegate() {
        return this.delegate;
    }

    /**
     * Wrap EntryProcessor if needed.
     * @param processor Processor.
     */
    public static <K,V,T> EntryProcessor<K,V,T> wrap(GridKernalContext ctx,
        @Nullable EntryProcessor<K,V,T> processor) {
        if (processor == null || processor instanceof EntryProcessorResourceInjectorProxy)
            return processor;

        GridResourceProcessor rsrcProcessor = ctx.resource();

        return rsrcProcessor.isAnnotationsPresent(null, processor, GridResourceIoc.AnnotationSet.ENTRY_PROCESSOR) ?
                new EntryProcessorResourceInjectorProxy<K,V,T>(processor) : processor;
    }

    /**
     * Unwrap EntryProcessor if needed.
     */
    public static <K,V,T> EntryProcessor<K,V,T> unwrap(EntryProcessor<K,V,T> processor) {
        return processor == null || !(processor instanceof EntryProcessorResourceInjectorProxy) ?
                processor :
                ((EntryProcessorResourceInjectorProxy)processor).getDelegate();
    }

    /**
     * Unwrap EntryProcessor as Object if needed.
     */
    public static Object unwrapAsObject(Object obj) {
        return obj == null || !(obj instanceof EntryProcessorResourceInjectorProxy) ?
            obj :
            ((EntryProcessorResourceInjectorProxy)obj).getDelegate();
    }
}

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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Continuous query handler V4 version.
 */
public class CacheContinuousQueryHandlerV4<K, V> extends CacheContinuousQueryHandlerV3<K, V> {
    /** */
    protected String locLsnrClsName;

    /** */
    private String locTransLsnrClsName;

    /**
     * Empty constructor.
     */
    public CacheContinuousQueryHandlerV4() {
        super();
    }

    /**
     * @param cacheName Cache name.
     * @param topic Topic.
     * @param locLsnr Local listener.
     * @param locTransLsnr Local listener of transformed events.
     * @param rmtFilterFactory Remote filter factory.
     * @param rmtTransFactory Remote transformer factory.
     * @param oldValRequired OldValRequired flag.
     * @param sync Sync flag.
     * @param ignoreExpired IgnoreExpired flag.
     * @param ignoreClsNotFound IgnoreClassNotFoundException flag.
     */
    public CacheContinuousQueryHandlerV4(
            String cacheName,
            Object topic,
            @Nullable CacheEntryUpdatedListener<K, V> locLsnr,
            ContinuousQueryWithTransformer.EventListener<?> locTransLsnr,
            @Nullable Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory,
            Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?>> rmtTransFactory,
            boolean oldValRequired,
            boolean sync,
            boolean ignoreExpired,
            boolean ignoreClsNotFound) {
        super(
                cacheName,
                topic,
                locLsnr, locTransLsnr,
                rmtFilterFactory, rmtTransFactory,
                oldValRequired,
                sync,
                ignoreExpired,
                ignoreClsNotFound
        );
    }

    /** {@inheritDoc} */
    @Override public String localListenerClassName() {
        if (localListener() != null)
            return localListener().getClass().getName();

        return locLsnrClsName;
    }

    /** {@inheritDoc} */
    @Override public String localTransformedEventListenerClassName() {
        if (localTransformedEventListener() != null)
            return localTransformedEventListener().getClass().getName();

        return locTransLsnrClsName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(localListener() != null ? localListener().getClass().getName() : null);
        out.writeObject(localTransformedEventListener() != null ? localTransformedEventListener().getClass().getName() : null);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        locLsnrClsName = (String)in.readObject();
        locTransLsnrClsName = (String)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryHandlerV4.class, this);
    }
}

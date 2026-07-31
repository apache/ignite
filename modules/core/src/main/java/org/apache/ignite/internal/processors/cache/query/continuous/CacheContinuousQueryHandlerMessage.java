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

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/** */
@UseBinaryMarshaller
public class CacheContinuousQueryHandlerMessage<K, V> implements MarshallableMessage {
    /** Remote filter. */
    CacheEntryEventSerializableFilter<K, V> rmtFilter;

    /** Lever of own marshaling. */
    @Order(0)
    public boolean[] externalMarshaling;

    /** Deployable object for {@link #rmtFilter}. Is {@code null} if no external marsshalling used. */
    @Order(1)
    @Nullable CacheContinuousQueryDeployableObject rmtFilterDep;

    /** Marshalled {@link #rmtFilter} if {@link #rmtFilterDep} is {@code null}. */
    @Order(2)
    @Nullable byte[] rmtFilterBytes;

    /** Remote filter factory. */
    @Nullable Factory<? extends CacheEntryEventFilter> rmtFilterFactory;

    /** Deployable object for {@link #rmtFilterFactory}. Is {@code null} if no external marsshalling used. */
    @Order(3)
    CacheContinuousQueryDeployableObject rmtFilterFactoryDep;

    /** Marshalled {@link #rmtFilterFactory} if {@link #rmtFilterFactoryDep} is {@code null}. */
    @Order(4)
    @Nullable byte[] rmtFilterFactoryBytes;

    /** Remote transformer factory. */
    Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?>> rmtTransFactory;

    /** Deployable object for {@link #rmtTransFactory}. Is {@code null} if no external marsshalling used. */
    @Order(5)
    CacheContinuousQueryDeployableObject rmtTransFactoryDep;

    /** Marshalled {@link #rmtTransFactory} if {@link #rmtTransFactoryDep} is {@code null}. */
    @Order(6)
    @Nullable byte[] rmtTransFactoryBytes;

    /** Cache name. */
    @Order(7)
    String cacheName;

    /** Topic for ordered messages. */
    @Marshalled("topicBytes")
    Object topic;

    /** Marshalled {@link #topic}. */
    @Order(8)
    byte[] topicBytes;

    /** Internal flag. */
    @Order(9)
    boolean internal;

    /** Notify existing flag. */
    @Order(10)
    boolean notifyExisting;

    /** Old value required flag. */
    @Order(11)
    boolean oldValRequired;

    /** Synchronous flag. */
    @Order(12)
    boolean sync;

    /** Ignore expired events flag. */
    @Order(13)
    boolean ignoreExpired;

    /** Task name hash code. */
    @Order(14)
    int taskHash;

    /** */
    @Order(15)
    boolean keepBinary;

    /** Event types for JCache API. */
    @Order(16)
    byte types;

    /** External marshaling. */
    protected void marshalExternally(GridKernalContext ctx) throws IgniteCheckedException {
        if (requiresDeployment(rmtFilter))
            rmtFilterDep = marshalDeployable(rmtFilter, ctx);

        if (requiresDeployment(rmtFilterFactory))
            rmtFilterFactoryDep = marshalDeployable(rmtFilterFactory, ctx);

        if (requiresDeployment(rmtTransFactory))
            rmtTransFactoryDep = marshalDeployable(rmtTransFactory, ctx);
    }

    /** Allows to od additional work along marshaling {@code deployable}. */
    protected CacheContinuousQueryDeployableObject marshalDeployable(
        Object deployable,
        GridKernalContext ctx
    ) throws IgniteCheckedException {
        return new CacheContinuousQueryDeployableObject(deployable, ctx);
    }

    /** {@inheritDoc} */
    @Override public void marshal(Marshaller marsh) throws IgniteCheckedException {
        externalMarshaling = new boolean[3];

        if (rmtFilterDep == null && rmtFilter != null) {
            rmtFilterBytes = marsh.marshal(rmtFilter);

            externalMarshaling[0] = true;
        }

        if (rmtFilterFactoryDep == null && rmtFilterFactory != null) {
            rmtFilterFactoryBytes = marsh.marshal(rmtFilterFactory);

            externalMarshaling[1] = true;
        }

        if (rmtTransFactoryDep == null && rmtTransFactory != null) {
            rmtTransFactoryBytes = marsh.marshal(rmtTransFactory);

            externalMarshaling[2] = true;
        }
    }

    /** External unmarshaling. */
    protected void unmarshalExternally(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        if (rmtFilterDep != null)
            rmtFilter = unmarshalExternally(rmtFilterDep, nodeId, ctx);

        if (rmtFilterFactoryDep != null)
            rmtFilterFactory = unmarshalExternally(rmtFilterFactoryDep, nodeId, ctx);

        if (rmtTransFactoryDep != null)
            rmtTransFactory = unmarshalExternally(rmtTransFactoryDep, nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        assert externalMarshaling != null && externalMarshaling.length == 3;

        if (externalMarshaling[0]) {
            assert rmtFilterBytes != null;

            rmtFilter = marsh.unmarshal(rmtFilterBytes, clsLdr);
        }

        if (externalMarshaling[1]) {
            assert rmtFilterFactoryBytes != null;

            rmtFilterFactory = marsh.unmarshal(rmtFilterFactoryBytes, clsLdr);
        }

        if (externalMarshaling[2]) {
            assert rmtTransFactoryBytes != null;

            rmtTransFactory = marsh.unmarshal(rmtTransFactoryBytes, clsLdr);
        }
    }

    /** */
    protected <T> T unmarshalExternally(
        CacheContinuousQueryDeployableObject depObj,
        UUID nodeId,
        GridKernalContext ctx
    ) throws IgniteCheckedException {
        return depObj.unmarshal(nodeId, ctx);
    }

    /** */
    protected static boolean requiresDeployment(@Nullable Object obj) {
        return obj != null && !U.isGrid(obj.getClass());
    }
}

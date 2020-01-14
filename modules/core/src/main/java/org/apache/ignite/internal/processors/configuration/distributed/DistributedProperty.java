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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper of some serializable property providing ability of change this value across whole cluster.
 */
public class DistributedProperty<T extends Serializable> {
    /** Name of property. */
    private final String name;

    /** Property value. */
    protected volatile T val;

    /** Sign of attachment to the processor. */
    private volatile boolean attached = false;

    /** Listeners of property update. */
    private final ConcurrentLinkedQueue<DistributePropertyListener<T>> updateListeners = new ConcurrentLinkedQueue<>();

    /**
     * Specific consumer for update value in cluster. It is null when property doesn't ready to update value on cluster
     * wide.
     */
    @GridToStringExclude
    private volatile PropertyUpdateClosure clusterWideUpdater;

    /**
     * @param name Name of property.
     *
     */
    public DistributedProperty(String name) {
        this.name = name;
    }

    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @return {@code true} if value was successfully updated and {@code false} if cluster wide update was failed,
     * perhaps some concurrent operation was changed this value in same moment.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    public boolean propagate(T newVal) throws IgniteCheckedException {
        ensureClusterWideUpdateIsReady();

        clusterWideUpdater.update(name, newVal).get();

        return true;
    }

    /**
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     */
    private void ensureClusterWideUpdateIsReady() throws DetachedPropertyException, NotWritablePropertyException {
        if (!attached)
            throw new DetachedPropertyException(name);

        if (clusterWideUpdater == null)
            throw new NotWritablePropertyException(name);
    }

    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @return Future for update operation.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    public GridFutureAdapter<?> propagateAsync(T newVal) throws IgniteCheckedException {
        ensureClusterWideUpdateIsReady();

        return clusterWideUpdater.update(name, newVal);
    }

    /**
     * @return Current property value.
     */
    public T get() {
        return val;
    }

    /**
     * @param dfltVal Default value when current value is null.
     * @return Current property value.
     */
    public T getOrDefault(T dfltVal) {
        return val == null ? dfltVal : val;
    }

    /**
     * @return Name of property.
     */
    public String getName() {
        return name;
    }

    /**
     * @param listener Update listener.
     */
    public void addListener(DistributePropertyListener<T> listener) {
        updateListeners.add(listener);
    }

    /**
     * This property have been attached to processor.
     */
    void onAttached() {
        attached = true;
    }

    /**
     * On this property ready to be update on cluster wide.
     *
     * @param updater Consumer for update value across cluster.
     */
    void onReadyForUpdate(@NotNull PropertyUpdateClosure updater) {
        this.clusterWideUpdater = updater;
    }

    /**
     * Update only local value without updating remote cluster.
     *
     * @param newVal New value.
     */
    void localUpdate(Serializable newVal) {
        T oldVal = val;

        val = (T)newVal;

        updateListeners.forEach(listener -> listener.onUpdate(name, oldVal, val));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedProperty.class, this);
    }
}

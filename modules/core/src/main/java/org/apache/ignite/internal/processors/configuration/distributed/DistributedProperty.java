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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteThrowableBiConsumer;
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
    /**
     * Specific consumer for update value in cluster. It is null when property doesn't ready to update value on cluster
     * wide.
     */
    @GridToStringExclude
    private volatile IgniteThrowableBiConsumer<String, Serializable> clusterWideUpdater;

    /**
     * @param name Name of property.
     * @param initVal Initial value of property.
     */
    public DistributedProperty(String name, T initVal) {
        this.val = initVal;
        this.name = name;
    }

    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @return {@code true} if value was successfully updated and {@code false} if cluster wide update have not
     * permitted yet.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedProperty)} before this method.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    public boolean propagate(T newVal) throws IgniteCheckedException {
        if (!attached)
            throw new DetachedPropertyException(name);

        if (clusterWideUpdater == null)
            return false;

        clusterWideUpdater.accept(name, newVal);

        return true;
    }

    /**
     * @return Current property value.
     */
    public T value() {
        return val;
    }

    /**
     * @return Name of property.
     */
    public String getName() {
        return name;
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
    void onReadyForUpdate(@NotNull IgniteThrowableBiConsumer<String, Serializable> updater) {
        this.clusterWideUpdater = updater;
    }

    /**
     * Update only local value without updating remote cluster.
     *
     * @param newVal New value.
     */
    void localUpdate(Serializable newVal) {
        val = (T)newVal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedProperty.class, this);
    }
}

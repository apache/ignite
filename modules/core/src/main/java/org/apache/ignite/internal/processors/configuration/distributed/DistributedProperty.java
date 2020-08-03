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
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Public interface of distributed property usage.
 */
public interface DistributedProperty<T extends Serializable> {
    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @return {@code true} if value was successfully updated and {@code false} if cluster wide update was failed,
     * perhaps some concurrent operation was changed this value in same moment.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedChangeableProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    boolean propagate(T newVal) throws IgniteCheckedException;

    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @return Future for update operation.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedChangeableProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    GridFutureAdapter<?> propagateAsync(T newVal) throws IgniteCheckedException;

    /**
     * Change value across whole cluster.
     *
     * @param newVal Value which this property should be changed on.
     * @param expectedVal Value from which this property should be changed.
     * @return Future for update operation.
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedChangeableProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     * @throws IgniteCheckedException If failed during cluster wide update.
     */
    GridFutureAdapter<?> propagateAsync(T expectedVal, T newVal) throws IgniteCheckedException;

    /**
     * @return Current property value.
     */
    T get();

    /**
     * @param dfltVal Default value when current value is null.
     * @return Current property value.
     */
    T getOrDefault(T dfltVal);

    /**
     * @return Name of property.
     */
    String getName();

    /**
     * @param listener Update listener.
     */
    void addListener(DistributePropertyListener<? super T> listener);
}

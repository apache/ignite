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
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper of some serializable property providing ability of change this value across whole cluster.
 */
public class SimpleDistributedProperty<T extends Serializable> implements DistributedChangeableProperty<T> {
    /** Name of property. */
    private final String name;

    /** Property value. */
    protected volatile T val;

    /** Sign of attachment to the processor. */
    private volatile boolean attached = false;

    /** Listeners of property update. */
    private final ConcurrentLinkedQueue<DistributePropertyListener<? super T>> updateListeners = new ConcurrentLinkedQueue<>();

    /**
     * Specific consumer for update value in cluster. It is null when property doesn't ready to update value on cluster
     * wide.
     */
    @GridToStringExclude
    private volatile PropertyUpdateClosure clusterWideUpdater;

    /** Property value parser. */
    @GridToStringExclude
    private final Function<String, T> parser;

    /**
     * @param name Name of property.
     * @param parser Property value parser.
     */
    public SimpleDistributedProperty(String name, Function<String, T> parser) {
        this.name = name;
        this.parser = parser;
    }

    /** {@inheritDoc} */
    @Override public boolean propagate(T newVal) throws IgniteCheckedException {
        ensureClusterWideUpdateIsReady();

        clusterWideUpdater.update(name, newVal).get();

        return true;
    }

    /**
     * @throws DetachedPropertyException If this property have not been attached to processor yet, please call {@link
     * DistributedConfigurationProcessor#registerProperty(DistributedChangeableProperty)} before this method.
     * @throws NotWritablePropertyException If this property don't ready to cluster wide update yet, perhaps cluster is
     * not active yet.
     */
    private void ensureClusterWideUpdateIsReady() throws DetachedPropertyException, NotWritablePropertyException {
        if (!attached)
            throw new DetachedPropertyException(name);

        if (clusterWideUpdater == null)
            throw new NotWritablePropertyException(name);
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> propagateAsync(T newVal) throws IgniteCheckedException {
        ensureClusterWideUpdateIsReady();

        return clusterWideUpdater.update(name, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> propagateAsync(T expectedVal, T newVal) throws IgniteCheckedException {
        ensureClusterWideUpdateIsReady();

        return clusterWideUpdater.casUpdate(name, expectedVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public T get() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public T getOrDefault(T dfltVal) {
        return val == null ? dfltVal : val;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void addListener(DistributePropertyListener<? super T> listener) {
        updateListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void onAttached() {
        attached = true;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForUpdate(@NotNull PropertyUpdateClosure updater) {
        this.clusterWideUpdater = updater;
    }

    /** {@inheritDoc} */
    @Override public void localUpdate(Serializable newVal) {
        T oldVal = val;

        val = (T)newVal;

        updateListeners.forEach(listener -> listener.onUpdate(name, oldVal, val));
    }

    /** {@inheritDoc} */
    @Override public T parse(String str) {
        if (parser == null)
            throw new IgniteException("The parser is not specified for property [name=" + name + ']');

        return parser.apply(str);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SimpleDistributedProperty.class, this);
    }

    /**
     * @param val String to parse.
     * @return Integer value.
     */
    public static Integer parseNonNegativeInteger(String val) {
        if (val == null || val.trim().isEmpty())
            return null;

        int intVal = Integer.parseInt(val);

        if (intVal < 0)
            throw new IllegalArgumentException("The value must not be negative");

        return intVal;
    }

    /**
     * @param val String to parse.
     * @return Long value.
     */
    public static Long parseNonNegativeLong(String val) {
        if (val == null || val.trim().isEmpty())
            return null;

        long intVal = Long.parseLong(val);

        if (intVal < 0)
            throw new IllegalArgumentException("The value must not be negative");

        return intVal;
    }

    /**
     * @param val String value.
     * @return String set.
     */
    public static HashSet<String> parseStringSet(String val) {
        HashSet<String> set = new HashSet<>();

        if (val == null || val.trim().isEmpty())
            return set;

        String[] vals = val.split("\\W+");

        set.addAll(Arrays.asList(vals));

        return set;
    }
}

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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed enum implementation for storing into meta storage.
 * The implementation will serialize by its integer ordinal.
 *
 * @param <T> Type of enum.
 */
public class DistributedEnumProperty<T extends Enum> implements DistributedChangeableProperty<T> {
    /** This property stored enumerator as order. */
    private final SimpleDistributedProperty<Integer> internal;

    /** Function reflects an integer, which getting from meta storage, to an enumeration value. */
    private final IgniteClosure<Integer, T> fromOrdinalFunc;

    /** Function converts an enumeration value to an integer for stored in meta storage. */
    private final IgniteClosure<T, Integer> toOredinalFunc;

    /**
     * Property constructor.
     *
     * @param name Name of property.
     * @param fromOrdinalFunc Function reflects an integer to an enumiration value.
     */
    public DistributedEnumProperty(
        String name,
        IgniteClosure<Integer, T> fromOrdinalFunc
    ) {
        this(name, fromOrdinalFunc, (T value) -> {
            return value == null ? null : value.ordinal();
        });
    }

    /**
     * Property constructor.
     *
     * @param name Name of property.
     * @param fromOrdinalFunc Function reflects an integer to an enumiration value.
     * @param toOredinalFunc Function converts an enumeration value to an integer.
     */
    public DistributedEnumProperty(
        String name,
        IgniteClosure<Integer, T> fromOrdinalFunc,
        IgniteClosure<T, Integer> toOredinalFunc
    ) {
        this.internal = new SimpleDistributedProperty<>(name);
        this.fromOrdinalFunc = fromOrdinalFunc;
        this.toOredinalFunc = toOredinalFunc;
    }

    /** {@inheritDoc} */
    @Override public void onAttached() {
        internal.onAttached();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForUpdate(@NotNull PropertyUpdateClosure updater) {
        internal.onReadyForUpdate(updater);
    }

    /** {@inheritDoc} */
    @Override public void localUpdate(Serializable newVal) {
        if (newVal == null)
            internal.localUpdate(null);
        else if (newVal instanceof Enum)
            internal.localUpdate(ordinalOrNull((T)newVal));
        else if (newVal instanceof Integer)
            internal.localUpdate(newVal);
    }

    /** {@inheritDoc} */
    @Override public boolean propagate(T newVal) throws IgniteCheckedException {
        return internal.propagate(ordinalOrNull(newVal));
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> propagateAsync(T newVal) throws IgniteCheckedException {
        return internal.propagateAsync(ordinalOrNull(newVal));
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> propagateAsync(T expectedVal, T newVal) throws IgniteCheckedException {
        return internal.propagateAsync(ordinalOrNull(expectedVal), ordinalOrNull(newVal));
    }

    /** {@inheritDoc} */
    @Override public T get() {
        return fromOrdinalOrNull(internal.get());
    }

    /** {@inheritDoc} */
    @Override public T getOrDefault(T dfltVal) {
        T val = get();

        return val == null ? dfltVal : val;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return internal.getName();
    }

    /** {@inheritDoc} */
    @Override public void addListener(DistributePropertyListener<T> listener) {
        internal.addListener(new DistributePropertyListener<Integer>() {
            @Override public void onUpdate(String name, Integer oldVal, Integer newVal) {
                listener.onUpdate(name, fromOrdinalOrNull(oldVal), fromOrdinalOrNull(newVal));
            }
        });
    }

    /**
     * Determines ordinal by enum value, or returns null if enum value is {@code null}.
     *
     * @param val Enum value.
     * @return Ordinal or {@code null}.
     */
    private Integer ordinalOrNull(T val) {
        return val == null ? null : toOredinalFunc.apply(val);
    }

    /**
     * Returns enum value or {@code null} if ordinal is {@code null} or less zero.
     *
     * @param ord Ordinal or {@code null}.
     * @return Enum value or {@code null}.
     */
    private T fromOrdinalOrNull(Integer ord) {
        return ord != null && ord >= 0 ? fromOrdinalFunc.apply(ord) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedEnumProperty.class, this);
    }
}

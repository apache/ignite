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

package org.apache.ignite.internal.util;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for ignite internal tree.
 */
public interface IgniteTree<L, T> {
    /**
     * Put value in this tree.
     *
     * @param val Value to be associated with the specified key.
     * @return The previous value associated with key.
     * @throws IgniteCheckedException If failed.
     */
    public T put(T val) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param x Implementation specific argument, {@code null} always means that we need a full detached data row.
     * @param c Closure.
     * @throws IgniteCheckedException If failed.
     */
    public void invoke(L key, Object x, InvokeClosure<T> c) throws IgniteCheckedException;

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     * key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     *  key.
     * @throws IgniteCheckedException If failed.
     */
    public T findOne(L key) throws IgniteCheckedException;

    /**
     * Returns a cursor from lower to upper bounds inclusive.
     *
     * @param lower Lower bound or {@code null} if unbounded.
     * @param upper Upper bound or {@code null} if unbounded.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public GridCursor<T> find(L lower, L upper) throws IgniteCheckedException;

    /**
     * Returns a cursor from lower to upper bounds inclusive.
     *
     * @param lower Lower bound or {@code null} if unbounded.
     * @param upper Upper bound or {@code null} if unbounded.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached
     *     data row.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public GridCursor<T> find(L lower, L upper, Object x) throws IgniteCheckedException;

    /**
     * Returns a value mapped to the lowest key, or {@code null} if tree is empty
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public T findFirst() throws IgniteCheckedException;

    /**
     * Returns a value mapped to the greatest key, or {@code null} if tree is empty
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public T findLast() throws IgniteCheckedException;

    /**
     * Removes the mapping for a key from this tree if it is present.
     *
     * @param key Key whose mapping is to be removed from the tree.
     * @return The previous value associated with key, or null if there was no mapping for key.
     * @throws IgniteCheckedException If failed.
     */
    public T remove(L key) throws IgniteCheckedException;

    /**
     * Returns the number of elements in this tree.
     *
     * @return the number of elements in this tree
     * @throws IgniteCheckedException If failed.
     */
    public long size() throws IgniteCheckedException;

    /**
     *
     */
    interface InvokeClosure<T> {
        /**
         *
         * @param row Old row or {@code null} if old row not found.
         * @throws IgniteCheckedException If failed.
         */
        void call(@Nullable T row) throws IgniteCheckedException;

        /**
         * @return New row for {@link OperationType#PUT} operation.
         */
        T newRow();

        /**
         * @return Operation type for this closure or {@code null} if it is unknown.
         *      After method {@link #call(Object)} has been called, operation type must
         *      be know and this method can not return {@code null}.
         */
        OperationType operationType();
    }

    /**
     *
     */
    enum OperationType {
        /** */
        NOOP,

        /** */
        REMOVE,

        /** */
        PUT,

        /** */
        IN_PLACE
    }
}

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

package org.apache.ignite.internal.storage;

import org.jetbrains.annotations.Nullable;

/**
 * Closure that performs an operation on the storage.
 *
 * @param <T> Type of the invocation's result.
 */
public interface InvokeClosure<T> {
    /**
     * In this method closure decides what type of operation should be performed on the storage, based on the current data in the storage
     * passed as an argument. The result of the operation can be obtained via the {@link #result()} method.
     *
     * @param row Old row or {@code null} if no old row has been found.
     */
    void call(@Nullable DataRow row);

    /**
     * @return Result of the invocation. Can be {@code null}.
     */
    @Nullable
    default T result() {
        return null;
    }

    /**
     * @return New row for the {@link OperationType#WRITE} operation.
     */
    @Nullable DataRow newRow();

    /**
     * @return Operation type for this closure or {@code null} if it is unknown. After method {@link #call(DataRow)} has been called,
     *      operation type must be computed and this method cannot return {@code null}.
     */
    @Nullable OperationType operationType();
}

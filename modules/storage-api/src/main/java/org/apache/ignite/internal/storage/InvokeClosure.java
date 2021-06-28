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

/** */
public interface InvokeClosure {
    /**
     * @param row Old row or {@code null} if the old row has not been found.
     */
    void call(@Nullable DataRow row);

    /**
     * @return New row for the {@link OperationType#WRITE} operation.
     */
    @Nullable DataRow newRow();

    /**
     * @return Operation type for this closure or {@code null} if it is unknown.
     * After method {@link #call(DataRow)} has been called, operation type must
     * be know and this method can not return {@code null}.
     */
    @Nullable OperationType operationType();
}

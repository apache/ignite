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

package org.apache.ignite.internal.storage.basic;

import java.util.Arrays;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Closure that deletes a specific data row with a given key and a given value.
 */
public class DeleteExactInvokeClosure implements InvokeClosure<Boolean> {
    /** Row to delete. */
    @NotNull
    private final DataRow row;

    /** {@code true} if can delete, {@code false} otherwise. */
    private boolean deletes = false;

    /**
     * Constructor.
     *
     * @param row Row to delete.
     */
    public DeleteExactInvokeClosure(@NotNull DataRow row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override
    public void call(@Nullable DataRow row) {
        deletes = row != null && Arrays.equals(this.row.valueBytes(), row.valueBytes());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable DataRow newRow() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable OperationType operationType() {
        return deletes ? OperationType.REMOVE : OperationType.NOOP;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Boolean result() {
        return deletes;
    }
}

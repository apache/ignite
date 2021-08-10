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

import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Closure that removes a data row with a given key and returns it.
 */
public class GetAndRemoveInvokeClosure implements InvokeClosure<Boolean> {
    /** Row that will be removed. */
    @Nullable
    private DataRow rowToRemove;

    /** {@code true} if can delete, {@code false} otherwise. */
    private boolean deletes;

    /** {@inheritDoc} */
    @Override public void call(@NotNull DataRow row) {
        this.rowToRemove = row;

        this.deletes = rowToRemove.hasValueBytes();
    }

    /** {@inheritDoc} */
    @Override public @Nullable DataRow newRow() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable OperationType operationType() {
        return deletes ? OperationType.REMOVE : OperationType.NOOP;
    }

    /**
     * @return Row that is removed.
     */
    @NotNull
    public DataRow oldRow() {
        assert rowToRemove != null;

        return rowToRemove;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Boolean result() {
        return deletes;
    }
}

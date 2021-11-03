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
 * Closure that replaces a data row with a given key and returns it.
 */
public class GetAndReplaceInvokeClosure implements InvokeClosure<Boolean> {
    /** New row. */
    @NotNull
    private final DataRow newRow;

    /** {@code true} if this closure should insert a new row only if a previous value exists. */
    private final boolean onlyIfExists;

    /** Previous data row. */
    @Nullable
    private DataRow oldRow;

    /** {@code true} if this closure replaces a row, {@code false} otherwise. */
    private boolean replaces;

    /**
     * Constructor.
     *
     * @param newRow       New row.
     * @param onlyIfExists Whether to insert a new row only if a previous one exists.
     */
    public GetAndReplaceInvokeClosure(@NotNull DataRow newRow, boolean onlyIfExists) {
        this.newRow = newRow;
        this.onlyIfExists = onlyIfExists;
    }

    /** {@inheritDoc} */
    @Override
    public void call(@Nullable DataRow row) {
        oldRow = row;

        replaces = row != null || !onlyIfExists;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable DataRow newRow() {
        return newRow;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable OperationType operationType() {
        return replaces ? OperationType.WRITE : OperationType.NOOP;
    }

    /**
     * @return Previous data row.
     */
    @Nullable
    public DataRow oldRow() {
        return oldRow;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Boolean result() {
        return replaces;
    }
}

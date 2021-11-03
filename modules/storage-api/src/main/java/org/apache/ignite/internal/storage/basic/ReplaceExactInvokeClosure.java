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
 * Closure that replaces an exact row with a specified key and a specified value.
 */
public class ReplaceExactInvokeClosure implements InvokeClosure<Boolean> {
    /** Expected data row. */
    @NotNull
    private final DataRow expectedRow;

    /** New data row. */
    @NotNull
    private final DataRow newRow;

    /** {@code true} if this closure replaces a row, {@code false} otherwise. */
    private boolean replaces;

    /**
     * Constructor.
     *
     * @param expectedRow Expected row.
     * @param newRow      New row.
     */
    public ReplaceExactInvokeClosure(@NotNull DataRow expectedRow, @NotNull DataRow newRow) {
        this.expectedRow = expectedRow;
        this.newRow = newRow;
    }

    /** {@inheritDoc} */
    @Override
    public void call(@Nullable DataRow row) {
        replaces = row != null && Arrays.equals(row.valueBytes(), expectedRow.valueBytes());
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

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Boolean result() {
        return replaces;
    }
}

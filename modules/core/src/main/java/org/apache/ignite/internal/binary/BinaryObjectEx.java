/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.jetbrains.annotations.Nullable;

/**
 * Extended binary object interface.
 */
public interface BinaryObjectEx extends BinaryObject {
    /**
     * @return Type ID.
     */
    public int typeId();

    /**
     * Get raw type.
     *
     * @return Raw type
     * @throws BinaryObjectException If failed.
     */
    @Nullable public BinaryType rawType() throws BinaryObjectException;

    /**
     * Check if flag set.
     *
     * @param flag flag to check.
     * @return {@code true} if flag is set, {@code false} otherwise.
     */
    public boolean isFlagSet(short flag);
}

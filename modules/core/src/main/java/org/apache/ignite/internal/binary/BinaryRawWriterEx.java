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

import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Extended writer interface.
 */
public interface BinaryRawWriterEx extends BinaryRawWriter, AutoCloseable {
    /**
     * @param obj Object to write.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException;

    /**
     * @return Output stream.
     */
    public BinaryOutputStream out();

    /**
     * Cleans resources.
     */
    @Override public void close();

    /**
     * Reserve a room for an integer.
     *
     * @return Position in the stream where value is to be written.
     */
    public int reserveInt();

    /**
     * Write int value at the specific position.
     *
     * @param pos Position.
     * @param val Value.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    public void writeInt(int pos, int val) throws BinaryObjectException;
}

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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.jetbrains.annotations.Nullable;

/**
 * Interop target abstraction.
 */
public interface PlatformTarget {
    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param val Value.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    long processInLongOutLong(int type, long val) throws IgniteCheckedException;

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException;

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem) throws IgniteCheckedException;

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException;

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @throws IgniteCheckedException In case of exception.
     */
    PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader) throws IgniteCheckedException;

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param arg Argument.
     * @param reader Binary reader.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    PlatformTarget processInObjectStreamOutObjectStream(int type, @Nullable PlatformTarget arg, BinaryRawReaderEx reader,
        BinaryRawWriterEx writer) throws IgniteCheckedException;

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException;

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @throws IgniteCheckedException In case of exception.
     */
    PlatformTarget processOutObject(int type) throws IgniteCheckedException;

    /**
     * Process asynchronous operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Async result (should not be null).
     * @throws IgniteCheckedException In case of exception.
     */
    PlatformAsyncResult processInStreamAsync(int type, BinaryRawReaderEx reader) throws IgniteCheckedException;

    /**
     * Convert caught exception.
     *
     * @param e Exception to convert.
     * @return Converted exception.
     */
    Exception convertException(Exception e);
}
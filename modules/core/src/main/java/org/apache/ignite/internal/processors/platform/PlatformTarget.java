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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.jetbrains.annotations.Nullable;

/**
 * Interop target abstraction.
 */
@SuppressWarnings("UnusedDeclaration")
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
     * Convert caught exception.
     *
     * @param e Exception to convert.
     * @return Converted exception.
     */
    Exception convertException(Exception e);
}
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
import org.jetbrains.annotations.Nullable;

/**
 * Interop target abstraction.
 */
@SuppressWarnings("UnusedDeclaration")
public interface PlatformTarget {
    /**
     * Operation accepting memory stream and returning long value.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @return Result.
     * @throws Exception If case of failure.
     */
    public long inStreamOutLong(int type, long memPtr) throws Exception;

    /**
     * Operation accepting memory stream and returning object.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @return Result.
     * @throws Exception If case of failure.
     */
    public Object inStreamOutObject(int type, long memPtr) throws Exception;

    /**
     * Operation accepting one memory stream and returning result to another memory stream.
     *
     * @param type Operation type.
     * @param inMemPtr Input memory pointer.
     * @param outMemPtr Output memory pointer.
     * @throws Exception In case of failure.
     */
    public void inStreamOutStream(int type, long inMemPtr, long outMemPtr) throws Exception;

    /**
     * Operation accepting an object and a memory stream and returning result to another memory stream.
     *
     * @param type Operation type.
     * @param arg Argument (optional).
     * @param inMemPtr Input memory pointer.
     * @param outMemPtr Output memory pointer.
     * @throws Exception In case of failure.
     */
    public void inObjectStreamOutStream(int type, @Nullable Object arg, long inMemPtr, long outMemPtr) throws Exception;

    /**
     * Operation returning long result.
     *
     * @param type Operation type.
     * @return Result.
     * @throws Exception In case of failure.
     */
    public long outLong(int type) throws Exception;

    /**
     * Operation returning result to memory stream.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @throws Exception In case of failure.
     */
    public void outStream(int type, long memPtr) throws Exception;

    /**
     * Operation returning object result.
     *
     * @param type Operation type.
     * @return Result.
     * @throws Exception If failed.
     */
    public Object outObject(int type) throws Exception;

    /**
     * Start listening for the future.
     *
     * @param futId Future ID.
     * @param typ Result type.
     * @throws IgniteCheckedException In case of failure.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void listenFuture(final long futId, int typ) throws Exception;

    /**
     * Start listening for the future for specific operation type.
     *
     * @param futId Future ID.
     * @param typ Result type.
     * @param opId Operation ID required to pick correct result writer.
     * @throws IgniteCheckedException In case of failure.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void listenFutureForOperation(final long futId, int typ, int opId) throws Exception;
}
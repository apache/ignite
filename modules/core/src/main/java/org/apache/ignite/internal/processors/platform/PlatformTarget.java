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

import org.jetbrains.annotations.*;

/**
 * Interop target abstraction.
 */
@SuppressWarnings("UnusedDeclaration")
public interface PlatformTarget {
    /**
     * Synchronous IN operation.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @return Value specific for the given operation otherwise.
     * @throws Exception If failed.
     */
    public int inOp(int type, long memPtr) throws Exception;

    /**
     * Synchronous IN operation which returns managed object as result.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @return Managed result.
     * @throws Exception If case of failure.
     */
    public Object inOpObject(int type, long memPtr) throws Exception;

    /**
     * Synchronous OUT operation.
     *
     * @param type Operation type.
     * @param memPtr Memory pointer.
     * @throws Exception In case of failure.
     */
    public void outOp(int type, long memPtr) throws Exception;

    /**
     * Synchronous IN-OUT operation.
     *
     * @param type Operation type.
     * @param inMemPtr Input memory pointer.
     * @param outMemPtr Output memory pointer.
     * @throws Exception In case of failure.
     */
    public void inOutOp(int type, long inMemPtr, long outMemPtr) throws Exception;

    /**
     * Synchronous IN-OUT operation with optional argument.
     *
     * @param type Operation type.
     * @param inMemPtr Input memory pointer.
     * @param outMemPtr Output memory pointer.
     * @param arg Argument (optional).
     * @throws Exception In case of failure.
     */
    public void inOutOp(int type, long inMemPtr, long outMemPtr, @Nullable Object arg) throws Exception;
}
